# stdlib

# third party
import streamlit as st
from langchain.chains import LLMChain
from langchain.output_parsers import PydanticOutputParser
from langchain.prompts import PromptTemplate
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain.schema.output_parser import OutputParserException
from langchain_openai import ChatOpenAI
from pydantic.v1.error_wrappers import ValidationError

# first party
from client import get_query_results
from helpers import create_graphql_code, create_tabs, to_arrow_table
from llm.examples import EXAMPLES
from llm.prompt import EXAMPLE_PROMPT
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


def set_openai_api_key():
    st.session_state._openai_api_key = st.session_state.openai_api_key


def set_question():
    previous_question = st.session_state.get("_question", None)
    st.session_state._question = st.session_state.question
    st.session_state.refresh = not previous_question == st.session_state._question


OPENAI_MODELS = {
    "gpt-3.5-turbo": {
        "description": "Most capable GPT-3.5 model, optimized for chat",
        "context_window": 4096,
        "cost_input": "$0.0015 / 1K tokens",
        "cost_output": "$0.002 / 1K tokens",
    },
    "gpt-3.5-turbo-16k": {
        "description": "Same capabilities as standard gpt-3.5-turbo with 4x the context length",
        "context_window": 16384,
        "cost_input": "$0.003 / 1K tokens",
        "cost_output": "$0.004 / 1K tokens",
    },
    "gpt-3.5-turbo-0125": {
        "description": "Updated GPT-3.5 Turbo model",
        "context_window": 16385,
        "cost_input": "$0.0005 / 1K tokens",
        "cost_output": "$0.0015 / 1K tokens",
    },
    "gpt-3.5-turbo-1106": {
        "description": "Updated GPT-3.5 Turbo model from November 2023",
        "context_window": 16385,
        "cost_input": "$0.001 / 1K tokens",
        "cost_output": "$0.002 / 1K tokens",
    },
    "gpt-4": {
        "description": "Most capable GPT-4 model, great for tasks that require advanced reasoning",
        "context_window": 8192,
        "cost_input": "$0.03 / 1K tokens",
        "cost_output": "$0.06 / 1K tokens",
    },
    "gpt-4-1106-preview": {
        "description": "Updated GPT-4 Turbo model with improved instruction following",
        "context_window": 128000,
        "cost_input": "$0.01 / 1K tokens",
        "cost_output": "$0.03 / 1K tokens",
    },
    "gpt-4o": {
        "description": "Most advanced multimodal model, faster and cheaper than GPT-4 Turbo with stronger capabilities",
        "context_window": 128000,
        "cost_input": "$5.00 / 1M tokens",
        "cost_output": "$5.00 / 1M tokens",
    },
    "gpt-4o-mini": {
        "description": "Affordable and intelligent small model for fast, lightweight tasks",
        "context_window": 128000,
        "cost_input": "$0.15 / 1M tokens",
        "cost_output": "$0.15 / 1M tokens",
    },
    "gpt-4-turbo-preview": {
        "description": "Most capable GPT-4 model, optimized for speed",
        "context_window": 128000,
        "cost_input": "$0.01 / 1K tokens",
        "cost_output": "$0.03 / 1K tokens",
    },
}

st.write("# LLM Query Builder")

st.markdown(
    "Input your OpenAI API Key and select a model in the sidebar to the left and ask "
    "questions abour your data.\n\n**This is highly experimental** and not meant to "
    "handle every edge case.  Please feel free to report any issues (or open up a PR "
    "to fix)."
)

api_key = st.sidebar.text_input(
    label="OpenAI API Key",
    type="password",
    value=st.session_state.get("_openai_api_key", ""),
    placeholder="Enter your API Key",
    key="openai_api_key",
    on_change=set_openai_api_key,
)

OPENAI_MODEL_OPTIONS = list(OPENAI_MODELS.keys())
DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_MODEL_INDEX = OPENAI_MODEL_OPTIONS.index(DEFAULT_MODEL)

model_name = st.sidebar.selectbox(
    label="Select Model",
    options=OPENAI_MODEL_OPTIONS,
    index=DEFAULT_MODEL_INDEX,
)

question = st.text_input(
    label="Ask a question",
    placeholder="e.g. What is total revenue?",
    key="question",
    on_change=set_question,
)

metrics = ", ".join(list(st.session_state.metric_dict.keys()))
dimensions = ", ".join(list(st.session_state.dimension_dict.keys()))

parser = PydanticOutputParser(pydantic_object=Query)

prompt_example = PromptTemplate(
    template=EXAMPLE_PROMPT,
    input_variables=["metrics", "dimensions", "question", "result"],
)

prompt = FewShotPromptTemplate(
    examples=EXAMPLES,
    example_prompt=prompt_example,
    prefix="""Given a question involving a user's data, transform it into a structured query object.
                It's important to remember that in the 'orderBy' field, only one of 'metric' or 'groupBy' should be set, not both.
                Additionally, when adding items to the `where` field and the identifier contains multiple dunder (__) characters,
                you'll need to change how you specify the dimension.  An example of this is `customer_order__customer__customer_market_segment`.
                This needs to be represented as Dimension('customer__customer_market_segment').
                Another example is `order__customer__nation__nation_name`.  This needs to be represented as
                Dimension('nation__nation_name').  Use only the last primary_entity__dimension_name in the identifier.
                Here are some examples showing how to correctly and incorrectly structure a query based on a user's question.
                {format_instructions}
            """,
    suffix="Metrics: {metrics}\nDimensions: {dimensions}\nQuestion: {question}\nResult:\n",
    input_variables=["metrics", "dimensions", "question"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

if question and st.session_state.get("refresh", False):
    if st.session_state.get("_openai_api_key", None) is None:
        st.warning("Please enter your OpenAI API Key")
        st.stop()
    try:
        llm = ChatOpenAI(
            openai_api_key=st.session_state._openai_api_key,
            model_name=model_name,
            temperature=0,
        )
    except ValidationError as e:
        st.write(e)
        st.stop()

    chain = LLMChain(llm=llm, prompt=prompt)
    output = chain.run(metrics=metrics, dimensions=dimensions, question=question)
    try:
        query = parser.parse(output)
    except OutputParserException as e:
        st.error(e)
        st.stop()
    python_code = create_graphql_code(query)
    with st.expander("View API Request", expanded=False):
        st.code(python_code, language="python")
    payload = {"query": query.gql, "variables": query.variables}
    data = get_query_results(payload, source="streamlit-llm")
    df = to_arrow_table(data["arrowResult"])
    df.columns = [col.lower() for col in df.columns]
    st.session_state.query_llm = query
    st.session_state.df_llm = df
    st.session_state.compiled_sql_llm = data["sql"]

st.session_state.refresh = False
create_tabs(st.session_state, "llm")
