# stdlib
import base64
import json
import urllib.parse
from typing import List

# third party
import pyarrow as pa
import streamlit as st

# first party
from chart import create_chart
from client import ConnAttr
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


def create_python_sdk_code(query: Query) -> str:
    arguments = query.sdk
    arguments_str = ",\n".join([f"    {k}={v}" for k, v in arguments.items() if v])
    return f"""
from dbtc import dbtCloudClient

# Assumes that DBT_CLOUD_SERVICE_TOKEN is set as env var
client = dbtCloudClient(environment_id={st.session_state.conn.params['environmentid']})
qr = client.sl.query(\n{arguments_str}\n)

# result will be a pandas dataframe as a default
qr.result
"""


def convert_df(df, to="to_csv", index=False):
    return getattr(df, to)(index=index).encode("utf8")


def create_explorer_link(query):
    if "account_id" in st.session_state:
        _, col2 = st.columns([4, 1.5])
        url = url_for_explorer(query.metric_names)
        col2.page_link(url, label="View from dbt Explorer", icon="🕵️")


def create_tabs(state: st.session_state, suffix: str) -> None:
    keys = ["query", "df", "compiled_sql"]
    keys_with_suffix = [f"{key}_{suffix}" for key in keys]
    if all(key in state for key in keys_with_suffix):
        sql = getattr(state, f"compiled_sql_{suffix}")
        df = getattr(state, f"df_{suffix}")
        query = getattr(state, f"query_{suffix}")
        tab1, tab2, tab3 = st.tabs(["Chart", "Data", "SQL"])
        with tab1:
            create_chart(df, query, suffix)
        with tab2:
            st.dataframe(df, use_container_width=True)
        with tab3:
            st.code(sql, language="sql")
        # create_explorer_link(query)


def encode_dictionary(d):
    # Convert the dictionary to a JSON string
    json_string = json.dumps(d)

    # Convert the JSON string to bytes
    json_bytes = json_string.encode("utf-8")

    # Encode the bytes using Base64
    base64_bytes = base64.b64encode(json_bytes)

    # Convert the Base64 bytes back to string for easy storage/transmission
    base64_string = base64_bytes.decode("utf-8")

    return base64_string


def decode_string(s: str):
    # Convert the Base64 string back to bytes
    try:
        base64_bytes = s.encode("utf-8")
    except AttributeError:
        return None

    # Decode the Base64 bytes to get back the original bytes
    json_bytes = base64.b64decode(base64_bytes)

    # Convert bytes back to JSON string
    json_string = json_bytes.decode("utf-8")

    # Parse the JSON string back to a dictionary
    d = json.loads(json_string)

    return d


def set_context_query_param(params: List[str]):
    d = {k: st.session_state[k] for k in params if k in st.session_state}
    encoded = encode_dictionary(d)
    st.query_params = {"context": encoded}


def retrieve_context_query_param():
    context = st.query_params.get("context", None)
    return decode_string(context)


def get_access_url(conn: ConnAttr = None):
    if conn is None:
        conn = st.session_state.conn

    if conn.host.endswith(".semantic-layer"):
        host = conn.host.replace(".semantic-layer", "")
    elif conn.host.startswith("https://semantic-layer."):
        host = conn.host.replace("https://semantic-layer.", "https://")
    else:
        host = conn.host.replace(".semantic-layer.", ".")

    return host


def url_for_disco(conn: ConnAttr = None):
    access_url = get_access_url(conn)
    netloc = urllib.parse.urlparse(access_url).netloc
    netloc_split = netloc.split(".")
    if "us1" in netloc_split or "us2" in netloc_split:
        account_prefix = netloc_split[0]
        the_rest = ".".join(netloc_split[1:])
        host = f"https://{account_prefix}.metadata.{the_rest}"
    else:
        host = f"https://metadata.{netloc}"
    return host


def url_for_explorer(metrics: List[str], *, conn: ConnAttr = None):
    if "account_id" not in st.session_state:
        return

    if conn is None:
        conn = st.session_state.conn

    metric_string = " ".join(f"+metric:{metric}" for metric in metrics)
    encoded_param = urllib.parse.quote_plus(metric_string)
    environment_id = conn.params["environmentid"]
    host = get_access_url(conn)
    full_url = f"{host}/explore/{st.session_state.account_id}/environments/{environment_id}/lineage/?select={encoded_param}"
    return full_url
