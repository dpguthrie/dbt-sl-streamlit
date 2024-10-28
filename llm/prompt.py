from langchain_core.prompts import (
    ChatPromptTemplate,
    FewShotPromptTemplate,
    MessagesPlaceholder,
    PromptTemplate,
)

from llm.examples import EXAMPLES

rephrase_system_prompt = (
    "Given a chat history and the latest user question "
    "which might reference context in the chat history, "
    "formulate a standalone question which can be understood "
    "without the chat history. Do NOT answer the question, "
    "just reformulate it if needed and otherwise return it as is."
)

rephrase_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", rephrase_system_prompt),
        MessagesPlaceholder("chat_history"),
        ("human", "{input}"),
    ]
)

intent_prompt = PromptTemplate.from_template(
    """
Based on the user's question, classify the intent as one of the following:
- "metadata" - This is for when the user is asking introspective-type questions about
  the underlying semantics defined in their project.  They want to know more about the
  metrics, dimensions, measures, and / or entities defined in their semantic layer.
  This will be helpful to them so they can understand what data they can query.
- "query" - This is for when the user actually wants to see data and query directly
  from the semantic layer.  In this question, the user is asking for something very
  specific related to metric(s) and/or dimension(s) defined in their project.
  
IMPORTANT:  Only return either "query" or "metadata".

Question:
{question}

Determine the intent:"""
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

QUERY_EXAMPLE_PROMPT = """
Available metrics: {metrics}.
Available dimensions: {dimensions}.

User question: {question}
Result: {result}
"""

query_prompt_example = PromptTemplate(
    template=QUERY_EXAMPLE_PROMPT,
    input_variables=["metrics", "dimensions", "question", "result"],
)

query_prompt = FewShotPromptTemplate(
    examples=EXAMPLES,
    example_prompt=query_prompt_example,
    prefix=prefix,
    suffix="Metrics: {metrics}\nDimensions: {dimensions}\nQuestion: {question}\nResult:\n",
    input_variables=["metrics", "dimensions", "question"],
)


metadata_prompt = ChatPromptTemplate.from_template(
    "Use this context to answer the user's question: {context} "
    "Your response should include only information contained within the "
    "provided context and should use appropriate markdown syntax."
    "User's question: {question}\nResponse:"
)
