# third party
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title='dbt Semantic Layer - View Metrics',
    page_icon='🌌',
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
from chart import create_chart
from client import submit_request
from helpers import get_shared_elements, to_arrow_table
from queries import GRAPHQL_QUERIES
from query import SemanticLayerQuery


def style_progress_bar(status: str):
    status_map = {'failed': '#FF2B2B', 'successful': '00FF74'}
    st.markdown(
        f"""
        <style>
            .st-hq {{
                background-color: {status_map[status]};
            }}
        </style>""",
        unsafe_allow_html=True,
    )


def get_time_length(interval):
    time_lengths = {
        'day': 1,
        'week': 7,
        'month': 30,
        'quarter': 90,
        'year': 365
    }
    return time_lengths.get(interval, 0)


def sort_by_time_length(time_intervals):
    return sorted(time_intervals, key=lambda x: get_time_length(x))


def add_where_state():
    st.session_state.where_items += 1
    

def subtract_where_state():
    st.session_state.where_items -= 1
    i = st.session_state.where_items
    for component in ['column', 'operator', 'condition', 'add', 'subtract']:
        where_component = f'where_{component}_{i}'
        if where_component in st.session_state:
            del st.session_state[where_component]
            
            
def add_order_state():
    st.session_state.order_items += 1
    

def subtract_order_state():
    st.session_state.order_items -= 1
    i = st.session_state.order_items
    for component in ['column', 'direction', 'add', 'subtract']:
        order_component = f'order_{component}_{i}'
        if order_component in st.session_state:
            del st.session_state[order_component]


# Initialize number of items in where clause
if 'where_items' not in st.session_state:
    st.session_state.where_items = 0

# Initialize number of items in order by clause
if 'order_items' not in st.session_state:
    st.session_state.order_items = 0


st.write('# View Your Metrics')

col1, col2 = st.columns(2)

# Retrieve metrics from dictionary
col1.multiselect(
    label='Select Metric(s)',
    options=sorted(st.session_state.metric_dict.keys()),
    default=None,
    key='selected_metrics',
    placeholder='Select a Metric'
)

# Retrieve unique dimensions based on overlap of metrics selected
all_dimensions = [
    v['dimensions'] for k, v in st.session_state.metric_dict.items()
    if k in st.session_state.selected_metrics
]
unique_dimensions = get_shared_elements(all_dimensions)

col2.multiselect(
    label='Select Dimension(s)',
    options=sorted(unique_dimensions),
    default=None,
    key='selected_dimensions',
    placeholder='Select a dimension'
)

# Only add grain if a time dimension has been selected
if len(unique_dimensions) > 0:
    dimension_types = set([
        st.session_state.dimension_dict[dim]['type'].lower()
        for dim in st.session_state.selected_dimensions
    ])
    if 'time' in dimension_types:
        col1, col2 = st.columns(2)
        grains = [
            st.session_state.metric_dict[metric]['queryableGranularities']
            for metric in st.session_state.selected_metrics
        ]
        col1.selectbox(
            label='Select Grain',
            options=sort_by_time_length(
                [g.strip().lower() for g in get_shared_elements(grains)]
            ),
            key='selected_grain',
        )

# Add sections for filtering and ordering
with st.expander('Filtering:'):
    if st.session_state.where_items == 0:
        st.button('Add Filters', on_click=add_where_state, key='static_filter_add')
    else:
        for i in range(st.session_state.where_items):
            col1, col2, col3, col4, col5 = st.columns([3, 1, 3, 1, 1])
            with col1:
                st.selectbox(
                    label='Select Column',
                    options=sorted(unique_dimensions),
                    key=f'where_column_{i}'
                )
            
            with col2:
                st.selectbox(
                    label='Operator',
                    options=[
                        '=', '>', '<', '>=', '<=', '<>', 'BETWEEN', 'LIKE', 'ILIKE', 'IN', 'NOT IN'
                    ],
                    key=f'where_operator_{i}',
                )
            
            with col3:
                st.text_input(
                    label='Condition',
                    value='',
                    key=f'where_condition_{i}'
                )
            
            with col4:
                st.button('Add', on_click=add_where_state, key=f'where_add_{i}')
            
            with col5:
                st.button(
                    'Remove',
                    on_click=subtract_where_state,
                    key=f'where_subtract_{i}',
                )

valid_orders = (
    st.session_state.selected_metrics + st.session_state.selected_dimensions
)
with st.expander('Ordering:'):
    if st.session_state.order_items == 0:
        st.button('Add Ordering', on_click=add_order_state, key='static_order_add')
    else:    
        for i in range(st.session_state.order_items):
            col1, col2, col3, col4 = st.columns([4, 2, 1, 1])
            with col1:
                st.selectbox(
                    label='Select Column',
                    options=sorted(valid_orders),
                    key=f'order_column_{i}'
                )
            
            with col2:
                st.selectbox(
                    label='Operator',
                    options=['ASC', 'DESC'],
                    key=f'order_direction_{i}',
                )
            
            with col3:
                st.button('Add', on_click=add_order_state, key=f'order_add_{i}')
            
            with col4:
                st.button(
                    'Remove', on_click=subtract_order_state, key=f'order_subtract_{i}'
                )
    

col1, col2 = st.columns(2)
col1.number_input(
    label='Limit Rows',
    min_value=0,
    value=0,
    key='selected_limit',
    help='Limit the amount of rows returned by the query with a limit clause',
)
col1.caption('If set to 0, no limit will be applied')

slq = SemanticLayerQuery(st.session_state)
jdbc_query = slq.jdbc_query
graphql_query = slq.graphql_query
tab1, tab2 = st.tabs(['GraphQL', 'JDBC'])
code = f'''
import requests


url = 'https://cloud.getdbt.com/semantic-layer/api/graphql'
query = \'\'\'{graphql_query}\'\'\'
payload = {{'query': query, 'variables': {slq._gql["variables"]}}}
response = requests.post(url, json=payload, headers={{'Authorization': 'Bearer ***'}})
'''
tab1.code(code, language='python')
tab2.code(jdbc_query, language='sql')

if st.button('Submit Query'):
    
    statuses = ['pending', 'running', 'compiled', 'failed', 'successful']
    
    if len(st.session_state.selected_metrics) == 0:
        st.warning('You must select at least one metric!')
        st.stop()

    progress_bar = st.progress(0, 'Submitting Query...')
    payload = {'query': graphql_query, 'variables': slq._gql['variables']}
    json = submit_request(st.session_state.conn, payload)
    try:
        query_id = json['data']['createQuery']['queryId']
    except TypeError:
        style_progress_bar('failed')
        progress_bar.progress(80, 'Query Failed!')
        st.error(json['errors'][0]['message'])
        st.stop()
    while True:
        query = GRAPHQL_QUERIES['get_results']
        payload = {'variables': {'queryId': query_id}, 'query': query}
        json = submit_request(st.session_state.conn, payload)
        try:
            data = json['data']['query']
        except TypeError:
            style_progress_bar('failed')
            progress_bar.progress(80, 'Query Failed!')
            st.error(json['errors'][0]['message'])
            st.stop()
        else:
            status = data['status'].lower()
            if status == 'successful':
                style_progress_bar('successful')
                progress_bar.progress(100, 'Query Successful!')
                break
            elif status == 'failed':
                progress_bar.progress(
                    (statuses.index(status) + 1) * 20,
                    'red:Query Failed!'
                )
                st.error(data['error'])
                st.stop()
            else:
                progress_bar.progress(
                    (statuses.index(status) + 1) * 20,
                    f'Query is {status.capitalize()}...'
                )
    
    df = to_arrow_table(data['arrowResult'])
    df.columns = [col.lower() for col in df.columns]
    st.session_state.slq = slq
    st.session_state.df = df
    st.session_state.compiled_sql = data['sql']

if 'df' in st.session_state and 'slq' in st.session_state:
    tab1, tab2, tab3 = st.tabs(['Chart', 'Data', 'SQL'])
    with tab1:
        create_chart(st.session_state.df, st.session_state.slq)

    with tab2:
        st.dataframe(st.session_state.df, use_container_width=True)

    with tab3:
        st.code(st.session_state.compiled_sql, language='sql')
