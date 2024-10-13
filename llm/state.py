# stdlib
from typing import Annotated

from langchain_core.messages import BaseMessage

# third party
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class State(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
