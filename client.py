# stdlib
from dataclasses import dataclass
from typing import Dict
from urllib.parse import parse_qs, urlparse

# third party
import requests
import streamlit as st

# first party
from queries import GRAPHQL_QUERIES


@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    params: dict  # {"environmentId": 42}
    auth_header: str  # "Bearer dbts_thisismyprivateservicetoken"


RESULT_STATUSES = ["pending", "running", "compiled", "failed", "successful"]


def submit_request(
    _conn_attr: ConnAttr,
    payload: Dict,
    source: str = None,
    host_override: str = None,
    path: str = "/api/graphql",
) -> Dict:
    # TODO: This should take into account multi-region and single-tenant
    url = f"{host_override or _conn_attr.host}{path}"
    if "variables" not in payload:
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


@st.cache_data(show_spinner=False)
def get_query_results(
    payload: Dict,
    source: str = None,
    key: str = "createQuery",
    progress: bool = True,
    conn: ConnAttr = None,
):
    conn = conn or st.session_state.conn
    if progress:
        progress_bar = st.progress(0, "Submitting Query ... ")
    json = submit_request(conn, payload, source=source)
    try:
        query_id = json["data"][key]["queryId"]
    except TypeError:
        if progress:
            progress_bar.progress(80, "Query Failed!")
        return json["errors"][0]["message"]

    while True:
        graphql_query = GRAPHQL_QUERIES["get_results"]
        results_payload = {"variables": {"queryId": query_id}, "query": graphql_query}
        json = submit_request(conn, results_payload)
        try:
            data = json["data"]["query"]
        except TypeError:
            if progress:
                progress_bar.progress(80, "Query Failed!")
            return json["errors"][0]["message"]

        status = data["status"].lower()
        if status == "successful":
            if progress:
                progress_bar.progress(100, "Query Successful!")
            break
        elif status == "failed":
            if progress:
                progress_bar.progress(
                    (RESULT_STATUSES.index(status) + 1) * 20, "red:Query Failed!"
                )
            return data["error"]

        else:
            if progress:
                progress_bar.progress(
                    (RESULT_STATUSES.index(status) + 1) * 20,
                    f"Query is {status.capitalize()}...",
                )

    return data
