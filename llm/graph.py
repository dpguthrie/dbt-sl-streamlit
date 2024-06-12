# stdlib
import operator
from typing import Annotated, Any, Dict, Sequence, TypedDict, Union

from langchain.agents import AgentExecutor, create_openai_functions_agent
from langchain_community.tools.tavily_search import TavilySearchResults

# third party
from langchain_core.messages import BaseMessage
from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser
from langchain_core.prompts import (
    ChatPromptTemplate,
    FewShotPromptTemplate,
    MessagesPlaceholder,
    PromptTemplate,
)
from langchain_core.runnables import RunnablePassthrough
from langchain_core.vectorstores import VectorStoreRetriever
from langchain_openai import ChatOpenAI
from langgraph.graph.message import StateGraph
from pydantic import BaseModel, Field

# first party
from llm.examples import EXAMPLES
from schema import Query


# Create the state used by the graph
class AgentState(TypedDict):
    input: str
    rephrased_question: str
    model: Union[ChatOpenAI]
    route: str
    chat_history: Annotated[Sequence[BaseMessage], operator.add]
    response: Dict[str, Any]
    retriever: VectorStoreRetriever


# Rephrase Step
rephrase_text = """
Given the following chat history and a follow up input, rephrase the follow up
input to be a standalone question.  Follow these rules:
- Do not answer the question
- If there is no chat history, simply use the input as the rephrased question
- Do not infer intent from the chat history or input.  For example, if the input
  is "What is total revenue?" and there is no chat history, the rephrased question
  should be "What is total revenue?".  Do not change this to "What is the definition
  of total revenue?"  This is not what the user asked.
- Do not add any additional context to the rephrased question
"""
rephrase_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", rephrase_text),
        MessagesPlaceholder("chat_history"),
        ("human", "{input}"),
    ]
)


def rephrase_question(state):
    print("---Rephrasing the user's question---")
    rephrase_chain = rephrase_prompt | state["model"] | StrOutputParser()
    rephrased_question = rephrase_chain.invoke(
        {"input": state["input"], "chat_history": state["chat_history"]}
    )
    print(f"-----New question: {rephrased_question}---")
    return {"rephrased_question": rephrased_question}


# Intent Step
class Router(BaseModel):
    """Call this to route the user to the appropriate next step."""

    choice: str = Field(description="should be one of: query, metadata, docs, other")


router_text = """You're a helpful assistant designed to take a user's question and
classify it into one of the following categories: query, metadata, docs, other.

- query: user is wanting to return actual data from their warehouse.  They want actual
  numbers for their metrics and dimensions to be returned.
- metadata: user wants to know what metrics and/or dimensions are available.  Or they're
    asking for information about the data that is available - like how are things
    calculated, defined, etc.
- docs: user is asking for information that can be found in dbt's documentation at
    docs.getdbt.com.  This is information that is not specific to the user's data but
    is more general information.
- other: user is asking a question that doesn't fit into any of the above categories.

Output only the category.  Do not answer the question.
"""

router_prompt = ChatPromptTemplate.from_messages(
    [("system", router_text), ("human", "{rephrased_question}")]
)


def find_route(state):
    print("---Finding next step based on rephrased question---")
    router_chain = router_prompt | state["model"] | StrOutputParser()
    route = router_chain.invoke({"rephrased_question": state["rephrased_question"]})
    print(f"-----Route: {route}")
    return {"route": route}


# Metadata Agent
metadata_template = """Answer the question based only on the following context:
{context}

If you're not able to answer the question with the available context, inform the
user of that.

Question: {question}
"""
metadata_prompt = ChatPromptTemplate.from_template(metadata_template)


def get_metadata(state):
    print("---Finding metadata.---")
    metadata_chain = (
        {"context": state["retriever"], "question": RunnablePassthrough()}
        | metadata_prompt
        | state["model"]
        | StrOutputParser()
    )
    response = metadata_chain.invoke(state["rephrased_question"])
    return {"response": {"metadata": response}}


# Docs Agent
docs_system_text = """You are a dbt expert.  Your sole purpose is to provide answers
related to dbt.  Your responses are well formatted, succint, and provide references to
images and source urls."""

nothing_returned = """
I'm sorry, I was unable to find anything related to your question.  Please try again.
"""

docs_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", docs_system_text),
        ("user", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]
)


def get_docs(state):
    print("---Finding docs.---")
    tools = [
        TavilySearchResults(
            max_results=3,
            include_images=True,
            include_domains=["getdbt.com"],
        )
    ]
    agent = create_openai_functions_agent(
        state["model"],
        tools,
        docs_prompt,
    )
    agent_executor = AgentExecutor(agent=agent, tools=tools)
    response = agent_executor.invoke({"input": state["rephrased_question"]})
    return {"response": {"docs": response.get("output", nothing_returned)}}


# Query Agent
initial_query_template = """
Available metrics: {metrics}.
Available dimensions: {dimensions}.

User question: {question}
Result: {result}
"""

initial_query_prompt = PromptTemplate(
    template=initial_query_template,
    input_variables=["metrics", "dimensions", "question", "result"],
)

query_parser = PydanticOutputParser(pydantic_object=Query)

few_shot_query_prompt = FewShotPromptTemplate(
    examples=EXAMPLES,
    example_prompt=initial_query_prompt,
    prefix="""
    Given a question involving a user's data, transform it into a structured query
    object.  Important things to note for this query object:
    - When adding items to the `where` field and the identifier contains multiple
      dunder (__) characters, you'll need to change how you specify the dimension.  An
      example of this is `customer_order__customer__customer_market_segment`. This needs
      to be represented as Dimension('customer__customer_market_segment').  Use only the
      last primary_entity__dimension_name in the identifier.
    - When using a metric to filter by, this is the syntax:
      {{{{ Metric('<metric_name>', ['<entity>']) }}}} <condition> <value> or when
      filtering by total profit on a customer's name:  `customer__customer_name`.  This
      becomes: {{{{ Metric('total_revenue', ['customer']) }}}} > 10000000
    - When a metric has metadata indicating that it requires_metric_time, always include
      `metric_time` in the `groupBy` field and use whatever you find in the
      `queryable_granularities` field for the `grain`.
    Here are some examples showing how to correctly and incorrectly structure a query based on a user's question.
    {format_instructions}
            """,
    suffix="Context: {context}\nQuestion: {question}\nResult:\n",
    input_variables=["context", "question"],
    partial_variables={"format_instructions": query_parser.get_format_instructions()},
)


def get_query(state):
    print("---Creating Query object---")
    query_chain = (
        {"context": state["retriever"], "question": RunnablePassthrough()}
        | few_shot_query_prompt
        | state["model"]
        | PydanticOutputParser(pydantic_object=Query)
    )
    response = query_chain.invoke(state["rephrased_question"])
    return {"response": {"query": response}}


def create_graph():
    graph = StateGraph(AgentState)
    graph.add_node("rephrase", rephrase_question)
    graph.add_node("router", find_route)
    graph.add_node("metadata", get_metadata)
    graph.add_node("query", get_query)
    graph.add_node("docs", get_docs)
    graph.add_conditional_edges("router", lambda state: state["route"])
    graph.set_entry_point("rephrase")
    graph.set_finish_point("metadata")
    graph.set_finish_point("query")
    graph.set_finish_point("docs")
    graph.add_edge("rephrase", "router")
    return graph.compile()
