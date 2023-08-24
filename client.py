# stdlib
from dataclasses import dataclass
from typing import Dict
from urllib.parse import parse_qs, urlparse

# third party
import requests
import streamlit as st


@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    params: dict  # {"environmentId": 42}
    auth_header: str  # "Bearer dbts_thisismyprivateservicetoken"
    

def submit_request(_conn_attr: ConnAttr, payload: Dict) -> Dict:

    # TODO: This should take into account multi-region and single-tenant
    url = 'https://cloud.getdbt.com/semantic-layer/api/graphql'
    if not 'variables' in payload:
        payload['variables'] = {}
    payload['variables']['environmentId'] = _conn_attr.params['environmentid']
    r = requests.post(
        url, json=payload, headers={'Authorization': _conn_attr.auth_header}
    )
    return r.json()


@st.cache_data
def get_connection_attributes(uri):
    """Helper function to convert the JDBC url into ConnAttr."""
    parsed = urlparse(uri)
    params = {k.lower(): v[0] for k, v in parse_qs(parsed.query).items()}
    try:
        token = params.pop('token')
    except KeyError:
        st.error('Token is missing from the JDBC URL.')
    else:
        return ConnAttr(
            host=parsed.path.replace("arrow-flight-sql", "grpc")
            if params.pop("useencryption", None) == "false"
            else parsed.path.replace("arrow-flight-sql", "grpc+tls"),
            params=params,
            auth_header=f"Bearer {token}",
        )
