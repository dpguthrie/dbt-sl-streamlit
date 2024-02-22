# stdlib
from dataclasses import dataclass
from typing import Dict
from urllib.parse import parse_qs, urlparse

# third party
import requests
import streamlit as st

# first party
from helpers import to_arrow_table
from queries import GRAPHQL_QUERIES
from schema import Query


RESULT_STATUSES = ["pending", "running", "compiled", "failed", "successful"]


@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    params: dict  # {"environmentId": 42}
    auth_header: str  # "Bearer dbts_thisismyprivateservicetoken"


def submit_request(_conn_attr: ConnAttr, payload: Dict, source: str = None) -> Dict:
    # TODO: This should take into account multi-region and single-tenant
    url = f"{_conn_attr.host}/api/graphql"
    if not "variables" in payload:
        payload["variables"] = {}
    payload["variables"]["environmentId"] = _conn_attr.params["environmentid"]
    r = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": _conn_attr.auth_header,
            "x-dbt-partner-source": source or "streamlit",
        },
    )
    return r.json()


@st.cache_data
def get_connection_attributes(uri):
    """Helper function to convert the JDBC url into ConnAttr."""
    parsed = urlparse(uri)
    params = {k.lower(): v[0] for k, v in parse_qs(parsed.query).items()}
    try:
        token = params.pop("token")
    except KeyError:
        st.error("Token is missing from the JDBC URL.")
    else:
        return ConnAttr(
            host=parsed.path.replace("arrow-flight-sql", "https").replace(":443", ""),
            params=params,
            auth_header=f"Bearer {token}",
        )


def get_query_results(payload: Dict, source: str = None):
    progress_bar = st.progress(0, "Submitting Query ... ")
    json = submit_request(st.session_state.conn, payload, source=source)
    try:
        query_id = json["data"]["createQuery"]["queryId"]
    except TypeError:
        progress_bar.progress(80, "Query Failed!")
        st.error(json["errors"][0]["message"])
        st.stop()
    while True:
        graphql_query = GRAPHQL_QUERIES["get_results"]
        results_payload = {"variables": {"queryId": query_id}, "query": graphql_query}
        json = submit_request(st.session_state.conn, results_payload)
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
                    (RESULT_STATUSES.index(status) + 1) * 20, "red:Query Failed!"
                )
                st.error(data["error"])
                st.stop()
            else:
                progress_bar.progress(
                    (RESULT_STATUSES.index(status) + 1) * 20,
                    f"Query is {status.capitalize()}...",
                )

    return data
