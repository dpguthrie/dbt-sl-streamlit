# stdlib
import time
from typing import Annotated, Union

# third party
import streamlit as st
from langchain.chat_models import init_chat_model
from langchain.prompts import ChatPromptTemplate, PromptTemplate
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser
from langchain_core.runnables import RunnableConfig, RunnablePassthrough
from langchain_core.tracers import LangChainTracer
from langchain_core.tracers.run_collector import RunCollectorCallbackHandler
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langsmith import Client
from streamlit_feedback import streamlit_feedback
from typing_extensions import TypedDict

# first party
from client import ConnAttr, get_query_results
from helpers import create_tabs, to_arrow_table
from llm.examples import EXAMPLES
from llm.prompt import EXAMPLE_PROMPT
from llm.providers import MODELS
from schema import Query

st.set_page_config(
    page_title="LLM",
    page_icon="🌌",
    layout="wide",
)

if "conn" not in st.session_state or st.session_state.conn is None:
    st.warning("Go to home page and enter your JDBC URL")
    st.stop()

if "metric_dict" not in st.session_state:
    st.warning(
        "No metrics found.  Ensure your project has metrics defined and a production "
        "job has been run successfully."
    )
    st.stop()


# On-Change Functions
def unset_llm_api_key():
    st.session_state._llm_api_key = None


def set_llm_api_key():
    st.session_state._llm_api_key = st.session_state.llm_api_key


def set_question():
    previous_question = st.session_state.get("_question", None)
    st.session_state._question = st.session_state.question
    st.session_state.refresh = not previous_question == st.session_state._question


# Graph
class State(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    route: str
    rephrased_question: str
    response: Union[str, Query]
    errors: int
    conn: ConnAttr


# Set up tracing via Lanngsmith
langchain_endpoint = "https://api.smith.langchain.com"
client = Client(api_url=langchain_endpoint, api_key=st.secrets["LANGCHAIN_API_KEY"])
ls_tracer = LangChainTracer(project_name="default", client=client)
run_collector = RunCollectorCallbackHandler()
cfg = RunnableConfig()
cfg["callbacks"] = [ls_tracer, run_collector]

# Initialize Streamlit Chat Message History
msgs = StreamlitChatMessageHistory(key="messages")

st.write("# Conversational Analytics")

st.markdown(
    """
Welcome to the natural language interface to your data!  Start asking questions like:
- What metrics can I query?
- What dimensions are available for revenue?
- Show me the top 10 customers by revenue
"""
)

# Sidebar
reset_history = st.sidebar.button("Reset chat history")
provider_name = st.sidebar.selectbox(
    label="Select Provider",
    options=list(MODELS.keys()),
    on_change=unset_llm_api_key,
)

MODEL_OPTIONS = list(MODELS[provider_name].keys())
DEFAULT_MODEL = [
    k for k, v in MODELS[provider_name].items() if v.get("default", False)
][0]
DEFAULT_MODEL_INDEX = MODEL_OPTIONS.index(DEFAULT_MODEL)

model_name = st.sidebar.selectbox(
    label="Select Model",
    options=MODEL_OPTIONS,
    index=DEFAULT_MODEL_INDEX,
)

MODEL_INFO = MODELS[provider_name][model_name]
st.sidebar.markdown(
    f"**Description**: {MODEL_INFO['description']}\n\n"
    f"**Context Window**: {MODEL_INFO['context_window']:,} Tokens\n\n"
    f"**Cost Input**: {MODEL_INFO['cost_input']}\n\n"
    f"**Cost Output**: {MODEL_INFO['cost_output']}"
)

api_key = st.sidebar.text_input(
    label="Provider API Key",
    type="password",
    value=st.session_state.get("_llm_api_key", ""),
    placeholder="Enter your API Key",
    key="llm_api_key",
    on_change=set_llm_api_key,
)

# Model Configuration
model_config = {
    "model": model_name,
    "model_provider": provider_name,
    "api_key": api_key,
}
# Initialize LLM
llm = init_chat_model(temperature=0, **model_config)
retriever = st.session_state.db.as_retriever(
    search_type="mmr",
    search_kwargs={
        "k": 6,
        "fetch_k": 20,
        "lambda_mult": 0.6,
    },
)


def query_node(state: State) -> Query:
    prompt = ChatPromptTemplate.from_template(
        "Use this context to answer the user's question: {context} "
        "Your response should include only information contained within the "
        "provided context and should use appropriate markdown syntax."
        "User's question: {question}\nResponse:"
    )
    prompt_example = PromptTemplate(
        template=EXAMPLE_PROMPT,
        input_variables=["metrics", "dimensions", "question", "result"],
    )

    prefix = """
    Given a both a question from a user and the semantic definition of metrics and 
    dimensions from that user's dbt project, create a JSON object conforming to this:
    
    class Query(BaseModel):
        metrics: List[MetricInput]
        groupBy: Optional[List[GroupByInput]] = None
        where: Optional[List[WhereInput]] = None
        orderBy: Optional[List[OrderByInput]] = None
        limit: Optional[int] = None
        
    Important things to remember when constructing this:
    - The `orderBy` field can only be one thing.  So, if both a metric and dimension 
      are required to be ordered by, create two seperate OrderByInput objects.
    - When a metric has requires_metric_time set to True, you need to always group by
      metric_time, using the appropriate queryable_granularity also found in the metric.
    - When adding items to the `where` field and the identifier contains multiple dunder
      `__` characters, you'll need to only include the last <primary_entity__dimension_name>
      An example is `customer_order__customer__balance_segment`, which would be represented
      as Dimension('customer__balance_segment')
    - At least one metric must always be included.

    Ensure accuracy and alignment with user intent. Only return a JSON object.  Do not 
    make up your own metrics and dimensions - only use the context provided.
    Examples follow:
    """

    prompt = FewShotPromptTemplate(
        examples=EXAMPLES,
        example_prompt=prompt_example,
        prefix=prefix,
        suffix="Context: {context}\n\nQuestion: {question}\nResult:\n",
        input_variables=["context", "question"],
    )

    chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt
        | llm
        | PydanticOutputParser(pydantic_object=Query)
    )
    query = chain.invoke(state["rephrased_question"])
    return {"response": query}


def metadata_node(state: State):
    prompt = ChatPromptTemplate.from_template(
        "Use this context to answer the user's question: {context} "
        "Your response should include only information contained within the "
        "provided context and should use appropriate markdown syntax."
        "User's question: {question}\nResponse:"
    )
    chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    response = chain.invoke(state["rephrased_question"])
    return {"response": response}


def route_node(state: State):
    prompt = ChatPromptTemplate.from_template(
        "Based on the user's question determine the user's intent.  The intent "
        "will either be to query the dbt Semantic Layer and see actual data or "
        "to understand the information contained in their semantic layer, "
        "specificially metrics and dimensions.  Only return one of the "
        "words based on the intent: 'query' or 'metadata'."
        "Again, only return the single word.\nUser's question: {question}"
    )
    chain = prompt | llm | StrOutputParser()
    response = chain.invoke({"question": state["rephrased_question"]})
    return {"route": response}


def rephrase_question(state: State):
    prompt = ChatPromptTemplate.from_template(
        "Given a user's newest question and the previous messages from the chat "
        "history, rephrase the question to include the relevant context, if "
        "any at all, from previous messages and return a single question "
        "that is both clear and concise.  Include context from previous messages when they "
        "relate to the user's current question or when it appears that it's building on "
        "top of previous questions.  Only return the rephrased question.\n"
        "User's question: {question}\n\nPrevious Messages: {messages}"
    )
    chain = prompt | llm | StrOutputParser()
    question = state["messages"][-1].content
    previous_messages = [m.content for m in state["messages"][:-1]]
    response = chain.invoke({"messages": previous_messages, "question": question})
    return {"rephrased_question": response}


def make_semantic_layer_request(state: State):
    query = state["response"]
    payload = {"query": query.gql, "variables": query.variables}
    data = get_query_results(
        payload, source="streamlit-llm", progress=False, conn=state["conn"]
    )
    if isinstance(data, str):
        errors = (state["errors"] or 0) + 1
        return {"response": data, "errors": errors}

    df = to_arrow_table(data["arrowResult"])
    df.columns = [col.lower() for col in df.columns]
    return {"response": {"query": state["response"], "df": df, "sql": data["sql"]}}


def redo_query(state: State):
    error = state["response"]
    prompt = ChatPromptTemplate.from_template(
        "There was an error creating the Query object.  The error returned from the API "
        "is the following:\n\n{error}\n\nYour job is to fix the error by creating a new "
        "rephrased question that will allow for a valid Query object to be created. "
        "As a reminder, you have access to the following context that is relevant "
        "when creating a Query object - a user's specific metrics and dimensions.  "
        "Here is the context: {context}.  Your response should only contain the "
        "rephrased question."
    )
    chain = (
        {"context": retriever, "error": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    response = chain.invoke(error)
    return {"rephrased_question": response}


builder = StateGraph(State)
builder.add_node("rephrase", rephrase_question)
builder.add_node("route_node", route_node)
builder.add_edge("rephrase", "route_node")
builder.set_entry_point("rephrase")
builder.add_node("query", query_node)
builder.add_node("semantic_layer_request", make_semantic_layer_request)
builder.add_edge("query", "semantic_layer_request")
builder.add_node("redo_query", redo_query)
builder.add_conditional_edges(
    "semantic_layer_request",
    lambda x: (
        "redo_query" if isinstance(x["response"], str) and x["errors"] < 3 else END
    ),
)
builder.add_node("metadata", metadata_node)
builder.add_conditional_edges(
    "route_node", lambda x: "query" if x["route"] == "query" else "metadata"
)
builder.add_edge("query", END)
builder.add_edge("metadata", END)

graph = builder.compile()

if len(msgs.messages) == 0 or reset_history:
    msgs.clear()
    msgs.add_ai_message("How can I help you?")
    st.session_state["last_run"] = None

avatars = {"human": "user", "ai": "assistant"}
human_messages = []
for msg in msgs.messages:
    if msg.type == "human":
        st.chat_message(avatars[msg.type]).write(msg.content)
        human_messages.append(msg.content)
    else:
        # Change to tool_call
        if (
            "route" in msg.additional_kwargs
            and msg.additional_kwargs["route"] == "query"
        ):
            create_tabs(st.session_state, msg.additional_kwargs["run_id"])
        else:
            st.chat_message(avatars[msg.type]).write(msg.content)

if input := st.chat_input(placeholder="What is total revenue in June?"):
    if st.session_state.get("_llm_api_key", None) is None:
        st.warning(f"Please enter your {provider_name} API Key")
        st.stop()
    msgs.add_user_message(input)
    human_messages.append(HumanMessage(content=input))
    st.chat_message("user").write(input)
    with st.status("Thinking...", expanded=True) as status:
        state_response = graph.invoke(
            {
                "messages": human_messages,
                "conn": st.session_state.conn,
            },
            cfg,
        )
    run_id = run_collector.traced_runs[0].id
    route = state_response["route"]
    with st.chat_message("assistant"):
        if route == "query":
            if isinstance(state_response["response"], str):
                content = state_response["response"]
                st.markdown(content)
                msgs.add_ai_message(
                    AIMessage(
                        content=content,
                        additional_kwargs={"run_id": run_id, "route": route},
                    )
                )
            else:
                query = state_response["response"]["query"]
                df = state_response["response"]["df"]
                sql = state_response["response"]["sql"]
                setattr(st.session_state, f"query_{run_id}", query)
                setattr(st.session_state, f"df_{run_id}", df)
                setattr(st.session_state, f"compiled_sql_{run_id}", sql)
                create_tabs(st.session_state, run_id)
                msgs.add_ai_message(
                    AIMessage(
                        content=str(query),
                        additional_kwargs={"run_id": run_id, "route": "query"},
                    )
                )
        else:
            content = state_response["response"]
            st.markdown(content)
            msgs.add_ai_message(
                AIMessage(
                    content=content,
                    additional_kwargs={"run_id": run_id, "route": route},
                )
            )
        st.session_state.last_run = run_id


@st.cache_data(ttl="2h", show_spinner=False)
def get_run_url(run_id):
    time.sleep(1)
    return client.read_run(run_id).url


if st.session_state.get("last_run"):
    # run_url = get_run_url(st.session_state.last_run)
    # st.sidebar.markdown(f"[Latest Trace: 🛠️]({run_url})")
    feedback = streamlit_feedback(
        feedback_type="thumbs",
        optional_text_label="[Optional] Please provide an explanation",
        key=f"feedback_{st.session_state.last_run}",
    )
    if feedback:
        scores = {"👎": 0, "👍": 1}
        client.create_feedback(
            st.session_state.last_run,
            feedback["type"],
            score=scores[feedback["score"]],
            comment=feedback.get("text", None),
        )
        st.toast("Feedback recorded!", icon="📝")
