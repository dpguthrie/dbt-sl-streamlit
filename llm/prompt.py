from langchain_core.prompts import (
    ChatPromptTemplate,
    FewShotPromptTemplate,
    MessagesPlaceholder,
    PromptTemplate,
)

from llm.examples import EXAMPLES

rephrase_system_prompt = """
You will be given a list of dialogues in a chat conversation in chronological order and a follow up question. Understand the flow of the conversation from the chat history and rephrase the question to be a standalone question.

The follow up question can do one of the two things,
1) Refinement: The follow up question is used to refine a query they asked previously, this could be adding or removing a filter or breaking down the query with a group by. For this, only use dialogues that are after the latest metric query. Be sure to include the exact name of last relevant metric in standalone question.
2) New Query: Switch context and start a new metric query. For this, do not use any of the previous dialogues.

If the follow up question is not related to any of the past questions, then return the follow up question as the standalone question without any rephrasing.
You are to reply with only the rephrased question. 
"""

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
- "metadata" - The user is asking about metadata, such as what metrics or dimensions are
  available, how a metric is defined, or similar / related metrics.
- "query" - The user is asking about data related to certain metrics.
  
IMPORTANT:  Only return either "query" or "metadata".

<Examples>
question: How many orders were made weekly?
answer: query

question: What metrics are related to revenue?
answer: metadata

question: Show me total revenue by month, balance segment, and nation.
answer: query
</Examples>

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
