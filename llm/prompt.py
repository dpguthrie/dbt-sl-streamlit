from langchain_core.prompts import (
    ChatPromptTemplate,
    MessagesPlaceholder,
    PromptTemplate,
)

EXAMPLE_PROMPT = """
Available metrics: {metrics}.
Available dimensions: {dimensions}.

User question: {question}
Result: {result}
"""


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

Conversation history:
{history}

Latest message:
{latest_message}

Determine the intent:"""
)
