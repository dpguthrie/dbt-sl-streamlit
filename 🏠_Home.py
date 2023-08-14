# stdlib
import json

# third party
import streamlit as st

# first party
from client import get_connection_attributes, submit_query
from jdbc_api import queries

        
def prepare_app():
    
    def _prepare_df(query: str, what: str):
        with st.spinner(f'Gathering {what.capitalize()}...'):
            df = submit_query(st.session_state.conn, query)
            if df is not None:
                df.columns = [col.lower() for col in df.columns]
                try:
                    df.set_index(keys='name', inplace=True)
                except KeyError:
                    
                    # Query worked, but nothing returned
                    return None
                return df    
        
    metric_df = _prepare_df(queries['metrics'], 'metrics')
    if metric_df is not None:
        metric_df['dimensions'] = metric_df['dimensions'].str.split(', ')
        metric_df['queryable_granularities'] = (
            metric_df['queryable_granularities'].str.split(', ')
        )
        metric_df['type_params'] = metric_df['type_params'].apply(
            lambda x: json.loads(x) if x else None
        )
        st.session_state.metric_dict = metric_df.to_dict(orient='index')
        dimension_df = _prepare_df(
            queries['dimensions'].format(
                **{'metrics': list(st.session_state.metric_dict.keys())}
            ),
            'dimensions'
        )
        dimension_df['type_params'] = dimension_df['type_params'].apply(
            lambda x: json.loads(x) if x else None
        )
        dimension_df = dimension_df[~dimension_df.index.duplicated(keep='first')]
        st.session_state.dimension_dict = dimension_df.to_dict(orient='index')
        st.success('Success!  Explore the rest of the app!')


st.set_page_config(
    page_title="dbt Semantic Layer - Home",
    page_icon="👋",
    
)
st.markdown('# Explore the dbt Semantic Layer')

st.markdown(
    """
    Use this app to query and view the metrics defined in your dbt project. It's important to note that this app assumes that you're using the new
    Semantic Layer, powered by [MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow).  The previous semantic layer used the `dbt_metrics`
    package, which has been deprecated and is no longer supported for `dbt-core>=1.6`.
    
    ---
    
    To get started, input your `JDBC_URL` below.  You can find this in your project settings when setting up the Semantic Layer.
    After hitting Enter, wait until a success message appears indicating that the application has successfully retrieved your project's metrics information.
    """
)


jdbc_url = st.text_input(
    label='JDBC URL',
    value='',
    key='jdbc_url',
    help='JDBC URL is found when configuring the semantic layer at the project level',
)

if st.session_state.jdbc_url != '':
    st.cache_data.clear()
    st.session_state.conn = get_connection_attributes(st.session_state.jdbc_url)
    if 'conn' in st.session_state and st.session_state.conn is not None:
        prepare_app()

st.markdown(
    """
    ---
    **👈 Now, select the "View Metrics" page from the sidebar** to query and view your metrics!

    ### Want to learn more?
    - Get started with the [dbt Semantic Layer](https://docs.getdbt.com/docs/use-dbt-semantic-layer/quickstart-sl)
    - Understand how to [build your metrics](https://docs.getdbt.com/docs/build/build-metrics-intro)
    - View the [Semantic Layer API](https://docs.getdbt.com/docs/dbt-cloud-apis/sl-api-overview)
"""
)
