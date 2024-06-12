# stdlib
import time

# third party
import streamlit as st
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tracers import LangChainTracer
from langchain_core.tracers.run_collector import RunCollectorCallbackHandler
from langchain_openai import ChatOpenAI
from langsmith import Client
from streamlit_feedback import streamlit_feedback

# first party
from client import get_query_results
from helpers import create_tabs, to_arrow_table
from llm.graph import create_graph

st.set_page_config(
    page_title="dbt Semantic Layer - View Metrics",
    page_icon="🌌",
    layout="wide",
)


if "conn" not in st.session_state or st.session_state.conn is None:
    st.warning("Go to home page and enter your JDBC URL")
    st.stop()

if "db" not in st.session_state or st.session_state.db is None:
    st.warning(
        "No metrics found.  Ensure your project has metrics defined and a production "
        "job has been run successfully."
    )
    st.stop()


# Initialize LangChain client
langchain_endpoint = "https://api.smith.langchain.com"
client = Client(api_url=langchain_endpoint, api_key=st.secrets["LANGCHAIN_API_KEY"])
ls_tracer = LangChainTracer(project_name="default", client=client)
run_collector = RunCollectorCallbackHandler()
cfg = RunnableConfig()
cfg["callbacks"] = [ls_tracer, run_collector]


msgs = StreamlitChatMessageHistory(key="chat_history")

st.write("# Conversational Analytics")

st.markdown(
    """
Welcome to the natural language interface to your data!  Start asking questions like:
- What metrics can I query?
- What dimensions are available for revenue?
- Show me the top 10 customers by revenue
- What's a semantic model?
"""
)

reset_history = st.sidebar.button("Reset chat history")
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
        if (
            "route" in msg.additional_kwargs
            and msg.additional_kwargs["route"] == "query"
        ):
            # create tabs
            create_tabs(st.session_state, msg.additional_kwargs["run_id"])
        else:
            st.chat_message(avatars[msg.type]).write(msg.content)

st.sidebar.text_input(
    "Databricks API Token",
    type="password",
    key="api_key",
    value=st.session_state.get("api_key", ""),
)

model = ChatOpenAI(
    model="gpt-4o", temperature=0, openai_api_key=st.secrets["OPENAI_API_KEY"]
)


def _is_none_or_blank(input: str):
    return input is None or input.strip() == ""


if input := st.chat_input(placeholder="What is total revenue?"):
    if _is_none_or_blank(st.session_state.api_key):
        st.error("Databricks API Token is required.")
        st.stop()
    msgs.add_user_message(input)
    st.chat_message("user").write(input)
    graph = create_graph()
    inputs = {
        "input": input,
        "chat_history": human_messages,
        "model": model,
        "retriever": st.session_state.db.as_retriever(),
    }
    with st.status("Thinking...", expanded=True) as status:
        data = graph.invoke(inputs, cfg)
    run_id = run_collector.traced_runs[0].id
    route = data["route"]
    with st.chat_message("assistant"):
        if route == "query":
            query = data["response"]["query"]
            payload = {
                "query": query.gql,
                "variables": query.variables,
            }
            data = get_query_results(payload, source="streamlit-llm")
            df = to_arrow_table(data["arrowResult"])
            df.columns = [col.lower() for col in df.columns]
            setattr(st.session_state, f"query_{run_id}", query)
            setattr(st.session_state, f"df_{run_id}", df)
            setattr(st.session_state, f"compiled_sql_{run_id}", data["sql"])
            create_tabs(st.session_state, run_id)
            msgs.add_ai_message(
                AIMessage(
                    content=str(query),
                    additional_kwargs={"run_id": run_id, "route": "query"},
                )
            )
        else:
            try:
                key = list(data["response"].keys())[0]
            except KeyError:
                content = "I'm sorry, I don't have an answer for that."
            else:
                content = data["response"][key]
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
