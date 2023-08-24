# third party
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


# stdlib
from typing import Dict

# third party
import pandas as pd
import plotly.express as px

# first party
from client import submit_request
from helpers import get_shared_elements, to_arrow_table
from queries import *


def _tabbed_queries(key: str, *, format: Dict = None, variables: Dict = None):
    tab1, tab2 = st.tabs(['GraphQL', 'JDBC'])
    with tab1:
        query = GRAPHQL_QUERIES[key]
        variables = variables.copy() if variables is not None else format.copy() if format is not None else {}
        variables['environmentId'] = int(st.session_state.conn.params['environmentid'])
        code = f'''
import requests


url = 'https://cloud.getdbt.com/semantic-layer/api/graphql'
query = \'\'\'{query}\'\'\'
payload = {{'query': query, 'variables': {variables}}}
response = requests.post(url, json=payload, headers={{'Authorization': 'Bearer ***'}})
        '''
        st.code(code, language='python')
        
    with tab2:
        query = JDBC_QUERIES[key]
        if format:
            query = query.format(**format)
        st.code(query, language='sql')
        
        
def _results_to_dataframe(results: Dict, key: str):
    try:
        data = results['data'][key]
    except TypeError:
        st.error(results['errors'][0]['message'])
        st.stop()
    else:
        return pd.DataFrame(data)


st.write('# Explore API')


tab1, tab2, tab3, tab4, tab5 = st.tabs([
    'Metrics',
    'Dimensions',
    'Dimension Values',
    'Time Granularities',
    'Common Metric Dimensions',
])


with tab1:
    st.info('Use this query to fetch all defined metrics in your dbt project.')
    query = GRAPHQL_QUERIES['metrics']
    _tabbed_queries('metrics')
    if st.button('Submit Query', key='explore_submit_1'):
        with st.spinner('Fetching metrics...'):
            results = submit_request(st.session_state.conn, {'query': query})
            df = _results_to_dataframe(results, 'metrics')
            df['dimensions'] = df['dimensions'].apply(lambda x: [d['name'] for d in x])
            df.set_index(keys='name', inplace=True)
            df.sort_values(by='name', inplace=True)
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
    query = GRAPHQL_QUERIES['dimensions']
    _tabbed_queries('dimensions', format={'metrics': metrics})
    if st.button('Submit Query', key='explore_submit_2'):
        if len(metrics) == 0:
            st.error('Please select at least one metric')
            st.stop()
        with st.spinner(f'Fetching dimensions for `{", ".join(metrics)}`'):
            payload = {'query': query, 'variables': {'metrics': metrics}}
            results = submit_request(st.session_state.conn, payload)
            df = _results_to_dataframe(results, 'dimensions')
            df.set_index(keys='name', inplace=True)
            df.sort_values(by='name', inplace=True)
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
    _tabbed_queries(
        'dimension_values',
        format={'metrics': metrics, 'dimension': [dimension]},
        variables={'metrics': metrics, 'groupBy': [dimension]}
    )
    if st.button('Submit Query', key='explore_submit_3'):
        with st.spinner(f'Fetching dimension values for `{dimension}`'):
            if len(metrics) == 0:
                st.error('Please select at least one metric')
                st.stop()
            query = GRAPHQL_QUERIES['dimension_values']
            payload = {
                'query': query, 'variables': {'metrics': metrics, 'groupBy': [dimension]}
            }
            json = submit_request(st.session_state.conn, payload)
            query_id = json['data']['createDimensionValuesQuery']['queryId']
            while True:
                query = GRAPHQL_QUERIES['get_results']
                payload = {'query': query, 'variables': {'queryId': query_id}}
                json = submit_request(st.session_state.conn, payload)
                try:
                    data = json['data']['query']
                except TypeError:
                    st.error(json['errors'][0]['message'])
                    st.stop()
                else:
                    if data['status'].lower() == 'successful':
                        break
                    
            df = to_arrow_table(data['arrowResult'])
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
    _tabbed_queries('queryable_granularities', format={'metrics': metrics})
    if st.button('Submit Query', key='explore_submit_4'):
        if len(metrics) == 0:
            st.error('Please select at least one metric')
            st.stop()
        query = GRAPHQL_QUERIES['queryable_granularities']
        payload = {'query': query, 'variables': {'metrics': metrics}}
        results = submit_request(st.session_state.conn, payload)
        df = _results_to_dataframe(results, 'queryableGranularities')
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
    _tabbed_queries('metrics_for_dimensions', format={'dimensions': dimensions})
    if st.button('Submit Query', key='explore_submit_5'):
        query = GRAPHQL_QUERIES['metrics_for_dimensions']
        payload = {'query': query, 'variables': {'dimensions': dimensions}}
        results = submit_request(st.session_state.conn, payload)
        df = _results_to_dataframe(results, 'metricsForDimensions')
        # Need to see if we can add metrics to the dataframe
        st.dataframe(df, use_container_width=True)
