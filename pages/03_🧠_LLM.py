# first party
import os

# third party
import streamlit as st
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI
from langchain.output_parsers import PydanticOutputParser
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain.schema.output_parser import OutputParserException
from pydantic.v1.error_wrappers import ValidationError


st.set_page_config(
    page_title="dbt Semantic Layer - View Metrics",
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


# first party
from client import submit_request
from helpers import to_arrow_table
from llm.examples import EXAMPLES
from llm.prompt import EXAMPLE_PROMPT
from llm.schema import Query
from queries import GRAPHQL_QUERIES


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
    prefix="""Given a question involving a user's data, transform it into a structured object.
    {format_instructions}
    """,
    suffix="Metrics: {metrics}\nDimensions: {dimensions}\nQuestion: {question}\nResult:\n",
    input_variables=["metrics", "dimensions", "question"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

st.write("# LLM Query Builder")

st.markdown(
    "Input your OpenAI API Key below and ask questions abour your data.\n\n**This is highly experimental** "
    "and not meant to handle every edge case.  Please feel free to report any issues (or open up a PR to fix)."
)

api_key = st.text_input(
    label="OpenAI API Key",
    type="password",
    key="openai_api_key",
    placeholder="Enter your OpenAI API Key",
)

question = st.text_input(
    label="Ask a question",
    placeholder="e.g. What is total revenue?",
    key="question",
)

if question:
    input = prompt.format(
        metrics=metrics,
        dimensions=dimensions,
        question=question,
    )
    try:
        llm = OpenAI(
            openai_api_key=os.environ.get("OPENAI_API_KEY", api_key),
            model_name="text-davinci-003",
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
    statuses = ["pending", "running", "compiled", "failed", "successful"]
    code = f"""
import requests


url = 'https://cloud.getdbt.com/semantic-layer/api/graphql'
query = \'\'\'{query.gql}\'\'\'
payload = {{'query': query, 'variables': {query.variables}}}
response = requests.post(url, json=payload, headers={{'Authorization': 'Bearer ***'}})
    """
    st.code(code, language="python")
    progress_bar = st.progress(0, "Submitting Query ... ")
    payload = {"query": query.gql, "variables": query.variables}
    json = submit_request(st.session_state.conn, payload)
    try:
        query_id = json["data"]["createQuery"]["queryId"]
    except TypeError:
        progress_bar.progress(80, "Query Failed!")
        st.error(json["errors"][0]["message"])
        st.stop()
    while True:
        query = GRAPHQL_QUERIES["get_results"]
        payload = {"variables": {"queryId": query_id}, "query": query}
        json = submit_request(st.session_state.conn, payload)
        try:
            data = json["data"]["query"]
        except TypeError:
            progress_bar.progress(80, "Query Failed!")
            st.error(json["errors"][0]["message"])
            st.stop()
        else:
            status = data["status"].lower()
            if status == "successful":
                progress_bar.progress(100, "Query Successful!")
                break
            elif status == "failed":
                progress_bar.progress(
                    (statuses.index(status) + 1) * 20, "red:Query Failed!"
                )
                st.error(data["error"])
                st.stop()
            else:
                progress_bar.progress(
                    (statuses.index(status) + 1) * 20,
                    f"Query is {status.capitalize()}...",
                )

    df = to_arrow_table(data["arrowResult"])
    df.columns = [col.lower() for col in df.columns]
    tab1, tab2 = st.tabs(["Data", "SQL"])
    with tab1:
        st.dataframe(df, use_container_width=True)
    with tab2:
        st.code(data["sql"], language="sql")
