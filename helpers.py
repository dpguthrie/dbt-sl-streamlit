# stdlib
import base64
from typing import Dict, List

# third party
import pyarrow as pa
import streamlit as st

# first party
from chart import create_chart
from schema import Query


def keys_exist_in_dict(keys_list, dct):
    return all(key in dct for key in keys_list)


def get_shared_elements(all_elements: List[List]):
    if len(all_elements) == 0:
        return []

    try:
        unique = set(all_elements[0]).intersection(*all_elements[1:])
    except IndexError:
        unique = set(all_elements[0])

    return list(unique)


def to_arrow_table(byte_string: str, to_pandas: bool = True) -> pa.Table:
    with pa.ipc.open_stream(base64.b64decode(byte_string)) as reader:
        arrow_table = pa.Table.from_batches(reader, reader.schema)

    if to_pandas:
        return arrow_table.to_pandas()

    return arrow_table


def create_graphql_code(query: Query) -> str:
    return f"""
import requests


url = '{st.session_state.conn.host}/api/graphql'
query = \'\'\'{query.gql}\'\'\'
payload = {{'query': query, 'variables': {query.variables}}}
response = requests.post(url, json=payload, headers={{'Authorization': 'Bearer ***'}})
    """


def create_tabs(state: st.session_state, suffix: str) -> None:
    keys = [f"query", "df", "compiled_sql"]
    keys_with_suffix = [f"{key}_{suffix}" for key in keys]
    if all(key in state for key in keys_with_suffix):
        sql = getattr(state, f"compiled_sql_{suffix}")
        df = getattr(state, f"df_{suffix}")
        query = getattr(state, f"query_{suffix}")
        tab1, tab2, tab3 = st.tabs(["Chart", "Data", "SQL"])
        with tab1:
            create_chart(df, query)
        with tab2:
            st.dataframe(df, use_container_width=True)
        with tab3:
            st.code(sql, language="sql")
