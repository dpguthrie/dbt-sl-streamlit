# stdlib
from datetime import datetime
from typing import Dict, List

# third party
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

# first party
from client import ConnAttr, get_connection_attributes, get_query_results
from helpers import to_arrow_table
from schema import Query

QUERIES = {
    "totals": {
        "metrics": [
            {"name": "total_revenue"},
            {"name": "total_expense"},
            {"name": "total_profit"},
        ],
        "groupBy": [
            {"name": "metric_time__day", "grain": "DAY"},
            {"name": "customer__nation"},
            {"name": "customer__region"},
            {"name": "customer__customer_balance_segment"},
            {"name": "customer__customer_market_segment"},
        ],
    },
    "expenses": {
        "metrics": [{"name": "total_expense"}],
        "groupBy": [
            {"name": "metric_time", "grain": "DAY"},
            {"name": "supplier__nation"},
            {"name": "supplier__supplier_name"},
            {"name": "customer_order__clerk_on_order"},
        ],
    },
}


@st.cache_data(show_spinner=False)
def retrieve_data(
    conn: ConnAttr,
    user_id: int,
    *,
    metrics: List[Dict] = None,
    groupBy: List[Dict] = None,
    where: List[Dict] = None,
    orderBy: List[Dict] = None,
    limit: int = None,
) -> Dict:
    user_filter = {"sql": f"{{{{ Dimension('customer__customer_id') }}}} = {user_id}"}
    if where is None:
        where = [user_filter]
    else:
        where.append(user_filter)
    query = Query(
        metrics=metrics or [],
        groupBy=groupBy or [],
        orderBy=orderBy or [],
        where=where,
        limit=limit,
    )
    payload = {"query": query.gql, "variables": query.variables}
    data = get_query_results(payload, progress=False, conn=conn)
    df = to_arrow_table(data["arrowResult"])
    df.columns = [col.lower() for col in df.columns]
    return {
        "df": df,
        "query": query,
    }


def create_filter_row(min_date: datetime, max_date: datetime):
    col1, col2 = st.columns(2)
    st.slider(
        "Date Range",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        key="date_range",
    )


def filter_df_by_date_range(
    df: pd.DataFrame, date_col: str = "metric_time__day"
) -> pd.DataFrame:
    min_date = st.session_state.date_range[0]
    max_date = st.session_state.date_range[1]
    return df[(df[date_col] >= min_date) & (df[date_col] <= max_date)]


def create_info_row(df: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Region", df.loc[0, "customer__region"].title())
    col2.metric("Country", df.loc[0, "customer__nation"].title())
    col3.metric(
        "Market Segment", df.loc[0, "customer__customer_market_segment"].title()
    )
    col4.metric(
        "Balance Segment", df.loc[0, "customer__customer_balance_segment"].title()
    )


def create_metrics_row(df: pd.DataFrame):
    metrics = [metric["name"] for metric in QUERIES["totals"]["metrics"]]
    col1, col2, col3 = st.columns(3)
    metrics_and_cols = {
        col1: {"name": metrics[0], "delta": 5},
        col2: {
            "name": metrics[1],
            "delta": -3,
            "delta_color": "inverse",
        },
        col3: {
            "name": metrics[2],
            "delta": 7,
        },
    }
    filtered_df = filter_df_by_date_range(df)
    result = filtered_df[metrics].sum()
    result["total_expense"] = result["total_revenue"] - result["total_profit"]
    for col, metric in metrics_and_cols.items():
        num = int(result.loc[metric["name"]])
        name = metric["name"].replace("_", " ").title()
        col.metric(
            name,
            f"${num:,}",
            # f'{metric["delta"]}%',
            # metric.get("delta_color", "normal"),
        )


def create_time_series_row(df: pd.DataFrame):
    col1, col2 = st.columns([0.2, 0.8])
    col1.selectbox(
        label="Select Grain",
        options=["Day", "Week", "Month", "Quarter", "Year"],
        index=0,
        key="ts_grain",
    )
    col1.selectbox(
        label="Select Aggregation",
        options=["Sum", "Mean", "Min", "Max"],
        index=0,
        key="ts_aggregation",
    )
    grain = st.session_state.ts_grain[0]
    agg = st.session_state.ts_aggregation.lower()
    df = filter_df_by_date_range(df)
    df["metric_time__day"] = pd.to_datetime(df["metric_time__day"])
    df["total_revenue"] = df["total_revenue"].astype(float)
    df = (
        df.groupby(pd.Grouper(key="metric_time__day", freq=grain))
        .agg({"total_revenue": agg})
        .reset_index()
    )
    col2.bar_chart(df, x="metric_time__day", y="total_revenue")


def create_top_n_row(df: pd.DataFrame, n: int = 5) -> None:
    def group_df(col: str, n: int):
        return (
            df.groupby(col)["total_expense"]
            .sum()
            .sort_values(ascending=False)
            .head(n)
            .reset_index()
        )

    col1, col2 = st.columns(2)
    df = filter_df_by_date_range(df)
    clerk_df = group_df("customer_order__clerk_on_order", n)
    clerk_df.columns = ["Clerk", "Total"]
    col1.subheader(f"Top {n} Clerks")
    col1.dataframe(clerk_df, use_container_width=True, hide_index=True)
    region_df = group_df("supplier__nation", n)
    region_df.columns = ["Region", "Total"]
    region_df["Region"] = region_df["Region"].apply(lambda x: x.title())
    col2.subheader(f"Top {n} Regions")
    col2.dataframe(region_df, use_container_width=True, hide_index=True)


def build_app(user_id: int):
    conn = get_connection_attributes(st.secrets["JDBC_URL"])
    initial_data = retrieve_data(conn, user_id, **QUERIES["totals"])
    expense_data = retrieve_data(conn, user_id, **QUERIES["expenses"])
    min_date = initial_data["df"]["metric_time__day"].min()
    max_date = initial_data["df"]["metric_time__day"].max()
    st.header("View the Customer's Dashboard")
    st.divider()
    st.subheader(f"Customer: {st.session_state['name']}")
    create_info_row(initial_data["df"])
    st.divider()
    create_filter_row(min_date, max_date)
    with st.container(border=True):
        st.subheader("Financial Metrics")
        create_metrics_row(initial_data["df"])
        create_time_series_row(initial_data["df"])
        st.subheader("Touchpoints")
        st.number_input(label="Top N", min_value=1, max_value=10, value=5, key="top_n")
        create_top_n_row(expense_data["df"], st.session_state.top_n)


st.set_page_config(
    page_title="dbt Semantic Layer - Embedded Analytics",
    page_icon="🧑‍💻",
    layout="wide",
)

with open("./config.yaml") as file:
    config = yaml.load(file, Loader=SafeLoader)

user_dict = {k: v["password"] for k, v in config["credentials"]["usernames"].items()}

st.title("Embedded Analytics")

st.write(
    """
The dbt Semantic Layer can also be used to power embedded analytics use cases for
organizations.  Embedded analytics allows companies to seamlessly integrate data
visualizations and insights directly into their own applications and products, providing
organizations the ability to empower their own users, increase engagement, and
(possibly) create new revenue streams.

By defining metrics in a dbt project, companies can ensure consistent, governed data is
available across all their applications - whether internal tools or customer-facing
products.  And, the Semantic Layer's [GraphQL API](
https://docs.getdbt.com/docs/dbt-cloud-apis/sl-graphql) makes it easy for downstream
applications to access and query the defined metrics and dimensions defined in that
dbt project, enabling a wide range of use cases.
"""
)

st.subheader("How can this work technically?")

st.write(
    """
The most crucial aspect of any embedded analytics application is ensuring the data
presented to each user is strictly limited to only the information they are authorized
to access.  This means that the data must be filtered based on the user's identity.  In
this example, we're able to apply a filter to each request to the GraphQL API based on
the logged in user:
"""
)
basic1, full1 = st.tabs(["Basic Example", "Full Example"])
basic1.code(
    body="""
user_filter = {"sql": f"{{ Dimension('customer__customer_id') }} = 5"}
    """,
    language="python",
)
full1.write(
    "Entrypoint is through the `retrieve_data` function.  Relevant lines are 89-92"
)
full1.code(
    body='''
# stdlib
import base64
import os
from typing import List, Dict

# third party
import pandas as pd
import pyarrow as pa
import requests


def submit_request(payload: Dict) -> Dict:
    # TODO: Update for your particular host
    url = "https://semantic-layer.cloud.getdbt.com/api/graphql"
    if "variables" not in payload:
        payload["variables"] = {}
    
    # TODO: Update for your environment ID
    payload["variables"]["environmentId"] = 1
    r = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": f"Token {os.getenv('DBT_CLOUD_SERVICE_TOKEN')}",
        },
    )
    return r.json()


def get_query_results(payload: Dict) -> Dict:
    json_data = submit_request(payload)
    try:
        query_id = json_data["data"]["createQuery"]["queryId"]
    except TypeError:
        error = json_data["errors"][0]["message"]
        print(error)
        raise

    graphql_query = """
        query GetResults($environmentId: BigInt!, $queryId: String!) {
            query(environmentId: $environmentId, queryId: $queryId) {
                arrowResult
                error
                queryId
                sql
                status
            }
        }
    """
    while True:

        results_payload = {"variables": {"queryId": query_id}, "query": graphql_query}
        json = submit_request(results_payload)
        try:
            json_data = json["data"]["query"]
        except TypeError:
            error = json_data["errors"][0]["message"]
            raise

        status = json_data["status"].lower()
        if status in ["successful", "failed"]:
            break

    return data



def to_arrow_table(
    byte_string: str, to_pandas: bool = True
) -> Union[pa.Table, pd.DataFrame]:
    with pa.ipc.open_stream(base64.b64decode(byte_string)) as reader:
        arrow_table = pa.Table.from_batches(reader, reader.schema)

    if to_pandas:
        return arrow_table.to_pandas()

    return arrow_table


def retrieve_data(
    user_id: int,
    *,
    metrics: List[Dict] = None,
    group_by: List[Dict] = None,
    where: List[Dict] = None,
    order_by: List[Dict] = None,
    limit: int = None,
) -> Dict:
    user_filter = {"sql": f"{{{{ Dimension('customer__customer_id') }}}} = {user_id}"}
    if where is None:
        where = []
    where.append(user_filter)
    mut = """
        mutation CreateQuery(
            $environmentId: BigInt!,
            $groupBy: [GroupByInput!]!,
            $limit: Int,
            $metrics: [MetricInput!]!,
            $orderBy: [OrderByInput!]!,
            $where: [WhereInput!]!,
        ) {
            createQuery(
                environmentId: $environmentId
                groupBy: $groupBy
                limit: $limit
                metrics: $metrics
                orderBy: $orderBy
                where: $where
            ) {
                queryId
            }
        }
    """
    variables = {
        "variables": {
            "metrics": metrics or [],
            "groupBy": group_by or [],
            "where": where,
            "orderBy": order_by or [],
            "limit": limit,
        }
    }
    payload = {"query": mut, "variables": variables}
    data = get_query_results(payload)
    df = to_arrow_table(data["arrowResult"])
    return df
    ''',
    language="python",
    line_numbers=True,
)

st.write(
    """
This filter is then applied to each query, ensuring that only the data relevant to the
logged in user is returned.  You can also imagine where this could be extended to
identify a user's company, role, department, or other attributes to further filter the
data.
    """
)
basic2, full2 = st.tabs(["Basic Example", "Full Example"])
basic2.code(
    body="""
company_filter = {"sql": f"{{ Dimension('customer__company__company_id') }} = 2"}
    """,
    language="python",
)
full2.write(
    "Entrypoint is through the `retrieve_data` function.  Relevant lines are 89-94"
)
full2.code(
    body='''
# stdlib
import base64
import os
from typing import List, Dict

# third party
import pandas as pd
import pyarrow as pa
import requests


def submit_request(payload: Dict) -> Dict:
    # TODO: Update for your particular host
    url = "https://semantic-layer.cloud.getdbt.com/api/graphql"
    if "variables" not in payload:
        payload["variables"] = {}
    
    # TODO: Update for your environment ID
    payload["variables"]["environmentId"] = 1
    r = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": f"Token {os.getenv('DBT_CLOUD_SERVICE_TOKEN')}",
        },
    )
    return r.json()


def get_query_results(payload: Dict) -> Dict:
    json_data = submit_request(payload)
    try:
        query_id = json_data["data"]["createQuery"]["queryId"]
    except TypeError:
        error = json_data["errors"][0]["message"]
        print(error)
        raise
        
    graphql_query = """
        query GetResults($environmentId: BigInt!, $queryId: String!) {
            query(environmentId: $environmentId, queryId: $queryId) {
                arrowResult
                error
                queryId
                sql
                status
            }
        }
    """
    while True:

        results_payload = {"variables": {"queryId": query_id}, "query": graphql_query}
        json = submit_request(results_payload)
        try:
            json_data = json["data"]["query"]
        except TypeError:
            error = json_data["errors"][0]["message"]
            raise

        status = json_data["status"].lower()
        if status in ["successful", "failed"]:
            break

    return data



def to_arrow_table(
    byte_string: str, to_pandas: bool = True
) -> Union[pa.Table, pd.DataFrame]:
    with pa.ipc.open_stream(base64.b64decode(byte_string)) as reader:
        arrow_table = pa.Table.from_batches(reader, reader.schema)

    if to_pandas:
        return arrow_table.to_pandas()

    return arrow_table


def retrieve_data(
    user: User,
    *,
    metrics: List[Dict] = None,
    group_by: List[Dict] = None,
    where: List[Dict] = None,
    order_by: List[Dict] = None,
    limit: int = None,
) -> Dict:
    user_filter = {
        "sql": f"{{{{ Dimension('customer__company__company_id') }}}} = {user.company.id}"
    }
    if where is None:
        where = []
    where.append(user_filter)
    mut = """
        mutation CreateQuery(
            $environmentId: BigInt!,
            $groupBy: [GroupByInput!]!,
            $limit: Int,
            $metrics: [MetricInput!]!,
            $orderBy: [OrderByInput!]!,
            $where: [WhereInput!]!,
        ) {
            createQuery(
                environmentId: $environmentId
                groupBy: $groupBy
                limit: $limit
                metrics: $metrics
                orderBy: $orderBy
                where: $where
            ) {
                queryId
            }
        }
    """
    variables = {
        "variables": {
            "metrics": metrics or [],
            "groupBy": group_by or [],
            "where": where,
            "orderBy": order_by or [],
            "limit": limit,
        }
    }
    payload = {"query": mut, "variables": variables}
    data = get_query_results(payload)
    df = to_arrow_table(data["arrowResult"])
    return df
    ''',
    language="python",
    line_numbers=True,
)

st.write(
    """
Another important element to any embedded analytics application is the ability to
retrieve data in a way that is performant and cost-efficient.  The dbt Semantic Layer
allows for a couple different [caching mechanisms](
https://docs.getdbt.com/docs/use-dbt-semantic-layer/sl-faqs#how-does-caching-work-in-the-dbt-semantic-layer)
that can help both reduce latency within the application as well as reduce the cost of
querying the underlying data warehouse.  In addition to caching, MetricFlow,
the underlying technology that powers the Semantic Layer, is designed to write [optimal SQL](
https://docs.getdbt.com/docs/use-dbt-semantic-layer/sl-faqs#how-are-queries-optimized-to-not-scan-more-data-than-they-should)
for your underlying data platform.
"""
)

st.markdown(
    """
**All of this put together - governance, caching, and optimized SQL - enables you to build
a performant, secure, and cost-effective embedded analytics application!**
"""
)
st.subheader("Give it a try!")

st.write(
    """
Use any of the credentials below in the login form to see how the dashboard changes based
on the user that is logged in.
"""
)

is_authenticated = st.session_state.get("authentication_status", False) or False

with st.expander("User Credentials", expanded=not is_authenticated):
    st.dataframe(
        pd.DataFrame(user_dict.items(), columns=["Username", "Password"]),
        hide_index=True,
        use_container_width=True,
    )

authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
    config["pre-authorized"],
)

authenticator.login()

if st.session_state["authentication_status"]:
    try:
        username = st.session_state["username"]
        name = st.session_state["name"]
        user_id = config["credentials"]["usernames"][username]["user_id"]
    except KeyError:
        pass
    else:
        build_app(user_id)
        st.write("Logout and try another user!")
    authenticator.logout()
elif st.session_state["authentication_status"] is False:
    st.error("Username/password is incorrect")
elif st.session_state["authentication_status"] is None:
    st.warning("Please enter your username and password")

st.markdown(
    """
<style>
[data-testid="stMetric"] {
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1), 0 1px 3px rgba(0, 0, 0, 0.08);
  border-radius: 8px;
  padding: 16px 24px;
  transition: box-shadow 0.3s ease-in-out;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
}

[data-testid="stMetric"]:hover {
  box-shadow: 0 10px 15px rgba(0, 0, 0, 0.1), 0 4px 6px rgba(0, 0, 0, 0.05);
}
</style>
""",
    unsafe_allow_html=True,
)
