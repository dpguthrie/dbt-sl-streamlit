# third party
import streamlit as st
from langchain.tools.retriever import create_retriever_tool

metadata_retriever_tool = create_retriever_tool(
    st.session_state.db.as_retriever(),
    "dbt_semantic_layer_metadata",
    (
        "Search through your dbt semantic layer metadata, which includes information "
        "about a customer's metrics, dimensions, and measures."
    ),
)
