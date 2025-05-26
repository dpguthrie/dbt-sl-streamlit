# stdlib
import os

# third party
import streamlit as st
from braintrust import init_logger
from braintrust_langchain import BraintrustCallbackHandler, set_global_handler
from langchain.chat_models import init_chat_model
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser
from langchain_core.runnables import RunnablePassthrough
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

# Initialize Braintrust logger and handler
logger = init_logger(project="Conversational Analytics", api_key=os.environ.get("BRAINTRUST_API_KEY"))
handler = BraintrustCallbackHandler()
set_global_handler(handler)


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

metadata = {
    "environment_id": st.session_state.conn.params["environmentid"],
    "host": st.session_state.conn.host,
    "provider_name": provider_name,
    "model_name": model_name,
}

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

# Create chains with custom names
rephrase_chain = (
    rephrase_prompt.with_config({"run_name": "Rephrase Prompt"}) 
    | llm.with_config({"run_name": "Rephrase LLM Call"}) 
    | StrOutputParser().with_config({"run_name": "Parse Rephrase Response"})
).with_config({"run_name": "Rephrase User Question"})

intent_chain = (
    intent_prompt.with_config({"run_name": "Intent Prompt"}) 
    | llm.with_config({"run_name": "Intent Classification LLM"}) 
    | StrOutputParser().with_config({"run_name": "Parse Intent Response"})
).with_config({"run_name": "Classify Intent"})

query_chain = (
    query_prompt.with_config({"run_name": "Query Generation Prompt"}) 
    | llm.with_config({"run_name": "Query Generation LLM"}) 
    | PydanticOutputParser(pydantic_object=Query).with_config({"run_name": "Parse GraphQL Query"})
).with_config({"run_name": "Generate GraphQL Query"})

retriever = st.session_state.db.as_retriever(
    search_type="mmr",
    search_kwargs={
        "k": 6,
        "fetch_k": 20,
        "lambda_mult": 0.6,
    },
).with_config({"run_name": "Retrieve Semantic Layer Metadata"})

metadata_chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | metadata_prompt.with_config({"run_name": "Metadata Prompt"})
    | llm.with_config({"run_name": "Metadata Generation LLM"})
    | StrOutputParser().with_config({"run_name": "Parse Metadata Response"})
).with_config({"run_name": "Generate Metadata Response"})

if input := st.chat_input(placeholder="What is total revenue in June?"):
    if st.session_state.get("_llm_api_key", None) is None:
        st.warning(f"Please enter your {provider_name} API Key")
        st.stop()

    msgs.add_user_message(input)
    
    # Start a single trace for the entire conversation
    with logger.start_span(name="Conversational Analytics", type="task") as conversation_span:
        conversation_span.log(input={"input": input, "chat_history": human_messages})
        
        with st.status("Thinking...", expanded=True) as status:
            # Step 1: Rephrase question
            with conversation_span.start_span(name="Rephrase Question", type="llm") as rephrase_span:
                st.write("Rephrasing question ... ")
                rephrase_span.log(input={"chat_history": human_messages, "input": input})
                question = rephrase_chain.invoke(
                    {"chat_history": human_messages, "input": input},
                    config={"run_name": "Rephrase User Question"}
                )
                rephrase_span.log(output=question)
            
            # Step 2: Determine intent
            with conversation_span.start_span(name="Determine Intent", type="llm") as intent_span:
                st.write("Determining intent...")
                intent_span.log(input={"question": question})
                intent = intent_chain.invoke(
                    {"question": question},
                    config={"run_name": "Classify Intent"}
                )
                intent_span.log(output=intent)
            
            if intent == "query":
                # Step 3a: Generate semantic layer query
                with conversation_span.start_span(name="Generate SL Query", type="llm") as query_span:
                    st.write("Creating semantic layer request...")
                    query_input = {
                        "metrics": ", ".join(list(st.session_state.metric_dict.keys())),
                        "dimensions": ", ".join(
                            list(st.session_state.dimension_dict.keys())
                        ),
                        "question": question,
                    }
                    query_span.log(input=query_input)
                    query = query_chain.invoke(
                        query_input, 
                        config={"run_name": "Generate GraphQL Query"}
                    )
                    query_span.log(output=query.model_dump())
                
                # Step 3b: Execute semantic layer query
                with conversation_span.start_span(name="Execute SL Query", type="function") as execute_span:
                    payload = {"query": query.gql, "variables": query.variables}
                    execute_span.log(input=payload)
                    st.write("Querying semantic layer...")
                    try:
                        data = get_query_results(payload, source="streamlit-llm")
                        execute_span.log(
                            output={"sql": data["sql"], "row_count": len(data.get("arrowResult", []))},
                            metadata={"source": "streamlit-llm"}
                        )
                    except Exception as e:
                        execute_span.log(error=str(e))
                        st.warning(e)
                        status.update(label="Failed", state="error")
                        st.stop()
                
                df = to_arrow_table(data["arrowResult"])
                df.columns = [col.lower() for col in df.columns]
                run_id = conversation_span.id
                setattr(st.session_state, f"query_{run_id}", query)
                setattr(st.session_state, f"df_{run_id}", df)
                setattr(st.session_state, f"compiled_sql_{run_id}", data["sql"])
                conversation_span.log(
                    output=query.model_dump(),
                    metadata={
                        "intent": intent,
                        "rephrased_question": question,
                        "chat_history": human_messages,
                        **metadata,
                    }
                )
            else:
                # Step 3: Retrieve metadata
                with conversation_span.start_span(name="Retrieve Metadata", type="llm") as metadata_span:
                    st.write("Retrieving metadata...")
                    metadata_span.log(input=input)
                    content = metadata_chain.invoke(
                        input, 
                        config={"run_name": "Generate Metadata Response"}
                    )
                    metadata_span.log(output=content)
                
                run_id = conversation_span.id
                conversation_span.log(
                    output=content, 
                    metadata={
                        "intent": intent,
                        "rephrased_question": question,
                        "chat_history": human_messages,
                        **metadata,
                    }
                )

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
    feedback = streamlit_feedback(
        feedback_type="thumbs",
        optional_text_label="[Optional] Please provide an explanation",
        key=f"feedback_{st.session_state.last_run}",
    )
    if feedback:
        scores = {"👎": 0, "👍": 1}
        score_value = scores[feedback["score"]]
        comment_text = feedback.get("text", None)
        
        # Build tags based on conditions
        tags = []
        if score_value == 0:
            tags.append("Triage")
        if comment_text and comment_text.strip():
            tags.append("User Comment")
        
        logger.log_feedback(
            id=str(st.session_state.last_run),
            scores={"user_feedback": score_value},
            comment=comment_text,
            tags=tags,
        )
        st.toast("Feedback recorded!", icon="📝")
