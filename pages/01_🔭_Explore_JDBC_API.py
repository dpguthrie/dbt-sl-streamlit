# third party
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title='dbt Semantic Layer - JDBC API',
    page_icon='🔭',
    layout='wide',
)

if 'conn' not in st.session_state or st.session_state.conn is None:
    st.warning('Go to home page and enter your JDBC URL')
    st.stop()
    
if 'metric_dict' not in st.session_state:
    st.warning(
        'No metrics found.  Ensure your project has metrics defined and a production '
        'job has been run successfully.'
    )
    st.stop()

# first party
from client import submit_query
from helpers import get_shared_elements
from jdbc_api import queries



tab1, tab2, tab3, tab4, tab5 = st.tabs([
    'Metrics',
    'Dimensions',
    'Dimension Values',
    'Time Granularities',
    'Common Metric Dimensions',
])

with tab1:
    st.info('Use this query to fetch all defined metrics in your dbt project.')
    query = queries['metrics']
    st.code(query)
    if st.button('Submit Query', key='explore_submit_1'):
        df = submit_query(st.session_state.conn, query, True)
        st.dataframe(df, use_container_width=True)
        
with tab2:
    st.info('Use this query to fetch all dimensions for a metric.')
    metrics = st.multiselect(
        label='Select Metric(s)',
        options=sorted(st.session_state.metric_dict.keys()),
        default=None,
        placeholder='Select a Metric',
        key='explore_metric_2'
    )
    query = queries['dimensions'].format(
        **{'metrics': metrics}
    )
    st.code(query)
    if st.button('Submit Query', key='explore_submit_2'):
        df = submit_query(st.session_state.conn, query, True)
        # Need to see if we can add metrics to the dataframe
        st.dataframe(df, use_container_width=True)
    
with tab3:
    st.info('Use this query to fetch dimension values for one or multiple metrics and single dimension.')
    metrics = st.multiselect(
        label='Select Metric(s)',
        options=sorted(st.session_state.metric_dict.keys()),
        default=None,
        placeholder='Select a Metric',
        key='explore_metric_3',
    )
    all_dimensions = [
        v['dimensions'] for k, v in st.session_state.metric_dict.items()
        if k in metrics
    ]
    unique_dimensions = get_shared_elements(all_dimensions)
    dimension = st.selectbox(
        label='Select Dimension',
        options=sorted(unique_dimensions),
        placeholder='Select a dimension'
    )
    query = queries['dimension_values'].format(
        **{'metrics': metrics, 'dimension': dimension}
    )
    st.code(query)
    if st.button('Submit Query', key='explore_submit_3'):
        df = submit_query(st.session_state.conn, query, True)
        # Need to see if we can add metrics to the dataframe
        st.dataframe(df, use_container_width=True)

with tab4:
    st.info('Use this query to fetch queryable granularities for a list of metrics. This argument allows you to only show the time granularities that make sense for the source model that the metrics are built off of.')
    metrics = st.multiselect(
        label='Select Metric(s)',
        options=sorted(st.session_state.metric_dict.keys()),
        default=None,
        placeholder='Select a Metric',
        key='explore_metric_4'
    )
    query = queries['queryable_granularities'].format(
        **{'metrics': metrics}
    )
    st.code(query)
    if st.button('Submit Query', key='explore_submit_4'):
        df = submit_query(st.session_state.conn, query, True)
        # Need to see if we can add metrics to the dataframe
        st.dataframe(df, use_container_width=True)
        
with tab5:
    st.info('Use this query to fetch available metrics given dimensions. This command is essentially the opposite of getting dimensions given a list of metrics.')
    all_dimensions = [v['dimensions'] for k, v in st.session_state.metric_dict.items()]
    unique_dimensions = get_shared_elements(all_dimensions)
    dimensions = st.multiselect(
        label='Select Dimension(s)',
        options=sorted(unique_dimensions),
        default=None,
        placeholder='Select a dimension'
    )
    query = queries['metrics_for_dimensions'].format(
        **{'dimensions': dimensions}
    )
    st.code(query)
    if st.button('Submit Query', key='explore_submit_5'):
        df = submit_query(st.session_state.conn, query, True)
        # Need to see if we can add metrics to the dataframe
        st.dataframe(df, use_container_width=True)
