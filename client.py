# stdlib
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

# third party
import streamlit as st
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect


@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    params: dict  # {"environmentId": 42}
    auth_header: str  # "Bearer dbts_thisismyprivateservicetoken"


@st.cache_data
def get_connection_attributes(uri):
    """Helper function to convert the JDBC url into ConnAttr."""
    parsed = urlparse(uri)
    params = {k.lower(): v[0] for k, v in parse_qs(parsed.query).items()}
    return ConnAttr(
        host=parsed.path.replace("arrow-flight-sql", "grpc")
        if params.pop("useencryption", None) == "false"
        else parsed.path.replace("arrow-flight-sql", "grpc+tls"),
        params=params,
        auth_header=f"Bearer {params.pop('token')}",
    )



@st.cache_data(show_spinner=False)
def submit_query(_conn_attr: ConnAttr, query: str):
    with connect(
        _conn_attr.host,
        db_kwargs={
            DatabaseOptions.AUTHORIZATION_HEADER.value: _conn_attr.auth_header,
            **{
                f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}{k}": v
                for k, v in _conn_attr.params.items()
            },
        },
    ) as conn, conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetch_df()  # fetches as Pandas DF, can also do fetch_arrow_table
    
    return df


if __name__ == "__main__":
    pass
