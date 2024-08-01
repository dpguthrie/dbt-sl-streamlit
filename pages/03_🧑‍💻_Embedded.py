# stdlib
from datetime import datetime
from typing import Dict, List

# third party
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from dbtsl import SemanticLayerClient
from yaml.loader import SafeLoader

# first party
from client import get_connection_attributes

st.set_page_config(
    page_title="dbt Semantic Layer - Embedded Analytics",
    page_icon="🧑‍💻",
    layout="wide",
)

QUERIES = {
    "totals": {
        "metrics": ["total_revenue", "total_expense", "total_profit"],
        "group_by": [
            "metric_time__day",
            "customer__nation",
            "customer__region",
            "customer__customer_balance_segment",
            "customer__customer_market_segment",
        ],
    },
    "expenses": {
        "metrics": ["total_expense"],
        "group_by": [
            "metric_time__day",
            "supplier__nation",
            "supplier__supplier_name",
            "customer_order__clerk_on_order",
        ],
    },
}

conn = get_connection_attributes(st.secrets["JDBC_URL"])

semantic_layer_client = SemanticLayerClient(
    environment_id=conn.params["environmentid"],
    auth_token=conn.auth_header.replace("Bearer ", ""),
    host=conn.host.replace("https://", ""),
)


@st.cache_data(show_spinner=False)
def retrieve_data(
    user_id: int,
    *,
    metrics: List[str] = None,
    group_by: List[str] = None,
    where: List[str] = None,
    order_by: List[str] = None,
    limit: int = None,
) -> Dict:
    user_filter = f"{{{{ Dimension('customer__customer_id') }}}} = {user_id}"
    if not where:
        where = [user_filter]
    else:
        where.append(user_filter)
    with semantic_layer_client.session():
        table = semantic_layer_client.query(
            metrics=metrics,
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=limit,
        )

    df = table.to_pandas()
    df.columns = [col.lower() for col in df.columns]
    return df


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
    metrics = QUERIES["totals"]["metrics"]
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
    initial_data_df = retrieve_data(user_id, **QUERIES["totals"])
    expense_data_df = retrieve_data(user_id, **QUERIES["expenses"])
    min_date = initial_data_df["metric_time__day"].min()
    max_date = initial_data_df["metric_time__day"].max()
    st.header("View the Customer's Dashboard")
    st.divider()
    st.subheader(f"Customer: {st.session_state['name']}")
    create_info_row(initial_data_df)
    st.divider()
    create_filter_row(min_date, max_date)
    with st.container(border=True):
        st.subheader("Financial Metrics")
        create_metrics_row(initial_data_df)
        create_time_series_row(initial_data_df)
        st.subheader("Touchpoints")
        st.number_input(label="Top N", min_value=1, max_value=10, value=5, key="top_n")
        create_top_n_row(expense_data_df, st.session_state.top_n)


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
full1.code(
    body="""
# stdlib
import os
from typing import List

# third party
from dbtsl import SemanticLayerClient


client = SemanticLayerClient(
    environment_id=1,
    auth_token=os.environ["DBT_CLOUD_SERVICE_TOKEN"],
    host="semantic-layer.cloud.getdbt.com",
)


def retrieve_data(
    user_id: int,
    *,
    metrics: List[str] = None,
    group_by: List[str] = None,
    where: List[str] = None,
    order_by: List[str] = None,
    limit: int = None,
) -> Dict:
    user_filter = f"{{{{ Dimension('customer__customer_id') }}}} = {user_id}"
    if not where:
        where = [user_filter]
    else:
        where.append(user_filter)
    with client.session():
        table = client.query(
            metrics=metrics,
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=limit,
        )

    return table
    """,
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
full2.code(
    body="""
# stdlib
import os
from typing import List

# third party
from dbtsl import SemanticLayerClient


client = SemanticLayerClient(
    environment_id=1,
    auth_token=os.environ["DBT_CLOUD_SERVICE_TOKEN"],
    host="semantic-layer.cloud.getdbt.com",
)


def retrieve_data(
    user_id: int,
    *,
    metrics: List[str] = None,
    group_by: List[str] = None,
    where: List[str] = None,
    order_by: List[str] = None,
    limit: int = None,
) -> Dict:
    company_filter = f"{{{{ Dimension('customer__company__company_id') }}}} = {user.company.id}"
    if not where:
        where = [company_filter]
    else:
        where.append(company_filter)
    with client.session():
        table = client.query(
            metrics=metrics,
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=limit,
        )

    return table
    """,
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
