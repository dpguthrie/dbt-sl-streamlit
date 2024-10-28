# third party
import streamlit as st
from langchain.chat_models import init_chat_model
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser
from langchain_core.runnables import RunnableConfig, RunnablePassthrough
from langchain_core.tracers import LangChainTracer
from langchain_core.tracers.run_collector import RunCollectorCallbackHandler
from langsmith import Client
from streamlit_feedback import streamlit_feedback

# first party
from client import get_query_results
from helpers import create_tabs, to_arrow_table
from llm.prompt import intent_prompt, metadata_prompt, query_prompt, rephrase_prompt
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


def unset_llm_api_key():
    st.session_state._llm_api_key = None


def set_llm_api_key():
    st.session_state._llm_api_key = st.session_state.llm_api_key


def set_question():
    previous_question = st.session_state.get("_question", None)
    st.session_state._question = st.session_state.question
    st.session_state.refresh = not previous_question == st.session_state._question


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

llm = init_chat_model(
    model_name,
    model_provider=provider_name,
    temperature=0,
    api_key=st.session_state.get("_llm_api_key", ""),
)

if len(msgs.messages) == 0 or reset_history:
    msgs.clear()
    msgs.add_ai_message("How can I help you?")
    st.session_state.last_run = None

avatars = {"human": "user", "ai": "assistant"}
human_messages = []
for msg in msgs.messages:
    if msg.type == "human":
        st.chat_message(avatars[msg.type]).write(msg.content)
        human_messages.append(msg.content)
    else:
        if (
            "route" in msg.additional_kwargs
            and msg.additional_kwargs["route"] == "query"
        ):
            create_tabs(st.session_state, msg.additional_kwargs["run_id"])
        else:
            st.chat_message(avatars[msg.type]).write(msg.content)

rephrase_chain = rephrase_prompt | llm | StrOutputParser()
intent_chain = intent_prompt | llm | StrOutputParser()
query_chain = query_prompt | llm | PydanticOutputParser(pydantic_object=Query)
retriever = st.session_state.db.as_retriever(
    search_type="mmr",
    search_kwargs={
        "k": 6,
        "fetch_k": 20,
        "lambda_mult": 0.6,
    },
)
metadata_chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | metadata_prompt
    | llm
    | StrOutputParser()
)

if input := st.chat_input(placeholder="What is total revenue in June?"):
    if st.session_state.get("_llm_api_key", None) is None:
        st.warning(f"Please enter your {provider_name} API Key")
        st.stop()

    msgs.add_user_message(input)
    with st.status("Thinking...", expanded=True) as status:
        st.write("Rephrasing question ... ")
        question = rephrase_chain.invoke(
            {"chat_history": human_messages, "input": input}, cfg
        )
        st.write("Determining intent...")
        intent = intent_chain.invoke({"question": question}, cfg)
        if intent == "query":
            st.write("Creating semantic layer request...")
            query = query_chain.invoke(
                {
                    "metrics": ", ".join(list(st.session_state.metric_dict.keys())),
                    "dimensions": ", ".join(
                        list(st.session_state.dimension_dict.keys())
                    ),
                    "question": question,
                }
            )
            payload = {"query": query.gql, "variables": query.variables}
            st.write("Querying semantic layer...")
            try:
                data = get_query_results(payload, source="streamlit-llm")
            except Exception as e:
                st.warning(e)
                status.update(label="Failed", state="error")
            df = to_arrow_table(data["arrowResult"])
            df.columns = [col.lower() for col in df.columns]
            run_id = run_collector.traced_runs[0].id
            setattr(st.session_state, f"query_{run_id}", query)
            setattr(st.session_state, f"df_{run_id}", df)
            setattr(st.session_state, f"compiled_sql_{run_id}", data["sql"])
        else:
            st.write("Retrieving metadata...")
            run_id = run_collector.traced_runs[0].id
            content = metadata_chain.invoke(input)

        status.update(label="Complete!", expanded=False, state="complete")

    human_messages.append(HumanMessage(content=input))
    st.session_state.last_run = run_id

    if intent == "query":

        create_tabs(st.session_state, run_id)
        msgs.add_ai_message(
            AIMessage(
                content=str(query),
                additional_kwargs={"run_id": run_id, "route": intent},
            )
        )
    else:
        st.markdown(content)
        msgs.add_ai_message(
            AIMessage(
                content=content,
                additional_kwargs={"run_id": run_id, "route": intent},
            )
        )

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
