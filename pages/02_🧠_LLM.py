# third party
import streamlit as st
from langchain.chat_models import init_chat_model
from langchain.output_parsers import PydanticOutputParser
from langchain.prompts import PromptTemplate
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain.schema.output_parser import OutputParserException
from pydantic.v1.error_wrappers import ValidationError

# first party
from client import get_query_results
from helpers import create_graphql_code, create_tabs, to_arrow_table
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


def unset_llm_api_key():
    st.session_state._llm_api_key = None


def set_llm_api_key():
    st.session_state._llm_api_key = st.session_state.llm_api_key


def set_question():
    previous_question = st.session_state.get("_question", None)
    st.session_state._question = st.session_state.question
    st.session_state.refresh = not previous_question == st.session_state._question


st.write("# LLM Query Builder")

st.markdown(
    "Input your OpenAI API Key and select a model in the sidebar to the left and ask "
    "questions abour your data.\n\n**This is highly experimental** and not meant to "
    "handle every edge case.  Please feel free to report any issues (or open up a PR "
    "to fix)."
)

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
    f"**Context Window**: {MODEL_INFO['context_window']}\n\n"
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

prefix = """
You are an AI assistant that creates SQL queries based on user input and dbt Semantic 
Layer context. Generate a JSON object, and only a JSON object, matching this Pydantic
model:

class Query(BaseModel):
    metrics: List[MetricInput]
    groupBy: Optional[List[GroupByInput]] = None
    where: Optional[List[WhereInput]] = None
    orderBy: Optional[List[OrderByInput]] = None
    limit: Optional[int] = None

Example JSON output from the question: "What is total revenue in 2023?":
{{
    "metrics": [ {{ "name": "total_revenue" }} ],
    "groupBy": null,
    "where": [ {{ "sql": "year({{{{ TimeDimension('metric_time', 'DAY') }}}}) = 2023" }} ],
    "orderBy": null,
    "limit": null 
}}

Ensure accuracy and alignment with user intent.  Only return a JSON object.
Examples follow:
"""

prompt = FewShotPromptTemplate(
    examples=EXAMPLES,
    example_prompt=prompt_example,
    prefix=prefix,
    suffix="Metrics: {metrics}\nDimensions: {dimensions}\nQuestion: {question}\nResult:\n",
    input_variables=["metrics", "dimensions", "question"],
)

if question and st.session_state.get("refresh", False):
    if st.session_state.get("_llm_api_key", None) is None:
        st.warning(f"Please enter your {provider_name} API Key")
        st.stop()
    try:
        llm = init_chat_model(
            model_name,
            model_provider=provider_name,
            temperature=0,
            api_key=st.session_state._llm_api_key,
        )
    except ValidationError as e:
        st.write(e)
        st.stop()

    try:
        chain = prompt | llm | parser
        query = chain.invoke(
            {
                "metrics": metrics,
                "dimensions": dimensions,
                "question": question,
            }
        )
    except OutputParserException as e:
        st.error(e)
        st.stop()
    except Exception as e:
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
