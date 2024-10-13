# third party
from langchain.output_parsers import PydanticOutputParser
from langchain.prompts import PromptTemplate
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain_core.tools import tool

# first party
from llm.examples import EXAMPLES
from llm.prompt import EXAMPLE_PROMPT
from schema import Query


def create_query_tool(llm, metrics: str, dimensions: str):
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

    Ensure accuracy and alignment with user intent. Only return a JSON object.
    Examples follow:
    """

    prompt = FewShotPromptTemplate(
        examples=EXAMPLES,
        example_prompt=prompt_example,
        prefix=prefix,
        suffix="Metrics: {metrics}\nDimensions: {dimensions}\nQuestion: {question}\nResult:\n",
        input_variables=["metrics", "dimensions", "question"],
    )
    chain = prompt | llm | PydanticOutputParser(pydantic_object=Query)

    @tool
    def dbt_semantic_layer_query_tool(question: str):
        """
        Create JSON objects matching the Pydantic model Query that are then used to
        query the dbt Semantic Layer and return data back to the user.

        Args:
            question (str): The user's question about the data.
        """
        return chain.invoke(
            {"question": question, "metrics": metrics, "dimensions": dimensions}
        )

    return dbt_semantic_layer_query_tool
