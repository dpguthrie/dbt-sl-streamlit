# third party
import pandas as pd
import plotly.express as px
import streamlit as st

# first party
from query import SemanticLayerQuery


CHART_TYPE_FIELDS = {
    'line': ['x', 'y', 'color', 'facet_row', 'facet_col', 'y2'],
    'bar': ['x', 'y', 'color', 'orientation', 'barmode', 'y2'],
    'pie': ['values', 'names'],
    'area': ['x', 'y', 'color', 'y2'],
    'scatter': ['x', 'y', 'color', 'size', 'facet_col', 'facet_row', 'trendline'],
    'histogram': ['x', 'nbins', 'histfunc'],
}


def _can_add_field(selections, available):
    return len(selections) < len(available)


def _available_options(selections, available):
    return [option for option in available if option not in selections]


def _sort_dataframe(df: pd.DataFrame, slq: SemanticLayerQuery):
    try:
        time_dimensions = [col for col in df.columns if col in slq.dimensions['time']]
    except KeyError:
        return df
    else:
        if len(time_dimensions) > 0:
            col = time_dimensions[0]
            is_sorted = df[col].is_monotonic_increasing
            if not is_sorted:
                df = df.sort_values(by=col)
        return df
    
    
def _add_secondary_yaxis(df, fig, dct):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    
    chart_map = {
        'line': 'Scatter',
        'bar': 'Bar',
        'area': 'Scatter',
    }
    configs = ['orientation']
    
    new_fig = make_subplots(specs=[[{"secondary_y": True}]])

    # add traces from plotly express figure to first figure
    for t in fig.select_traces():
        new_fig.add_trace(t, secondary_y = False)
        
    addl_config = {}
    if dct['chart_type'] == 'line':
        addl_config['mode'] = 'lines'
    elif dct['chart_type'] == 'area':
        addl_config['fill'] = 'tozeroy'

    new_fig.add_trace(
        getattr(go, chart_map[dct['chart_type']])(
            x = df[dct['x']],
            y = df[dct['y']],
            **addl_config
        ),
        secondary_y = True
    )
    return new_fig

        
def create_chart(df, slq: SemanticLayerQuery):
    col1, col2 = st.columns([.2, .8])
    
    # Create default chart types
    if slq.has_time_dimension:
        chart_types = ['line', 'area', 'bar']
    elif slq.has_entity_dimension:
        if len(slq.metrics) >= 2:
            chart_types = ['scatter', 'bar']
        else:
            chart_types = ['bar', 'pie', 'histogram']
    else:
        chart_types = ['bar', 'pie', 'histogram', 'scatter']

    selected_chart_type = col1.selectbox(
        label='Select Chart Type',
        options=chart_types,
        key='selected_chart_type',
    )

    chart_config = {}
    
    for field in CHART_TYPE_FIELDS[selected_chart_type]:
        
        selected_dimensions = [
            col for col in chart_config.values() if col in slq.all_dimensions
        ]
        selected_metrics = [
            col for col in chart_config.values() if col in slq.metrics
        ]
        
        
        if field == 'x':
            if selected_chart_type in ['scatter', 'histogram']:
                options = slq.metrics
            elif slq.has_time_dimension:
                options = slq.dimensions['time']
            else:
                options = slq.all_dimensions
            x = col1.selectbox(
                label='X-Axis',
                options=options,
                placeholder='Select Dimension',
                key='chart_config_x',
            )
            chart_config['x'] = x
            
        if field == 'y':
            if len(slq.metrics) == 1 or selected_chart_type != 'line':
                widget = 'selectbox'
                y_kwargs = {}
            else:
                widget = 'multiselect'
                y_kwargs = {'default': slq.metrics[0]}
            y = getattr(col1, widget)(
                label='Y-Axis',
                options=[m for m in slq.metrics if m not in chart_config.values()],
                placeholder='Select Metric',
                key='chart_config_y',
                **y_kwargs,
            )
            chart_config['y'] = y
            
        if (
            len(slq.metrics) > 1
            and field == 'y2'
            and len([m for m in slq.metrics if m not in chart_config.values()]) > 0
        ):
            chart_config['y2'] = {}
            expander = col1.expander('Secondary Axis Options')
            y2 = expander.selectbox(
                label='Secondary Axis',
                options=[None] + [m for m in slq.metrics if m not in chart_config.values()],
                key='chart_config_y2',
            )
            chart_config['y2']['metric'] = y2
            y2_chart = expander.selectbox(
                label='Secondary Axis Chart Type',
                options=chart_types,
                index=chart_types.index(selected_chart_type),
                key='chart_config_y2_chart_type'
            )
            chart_config['y2']['chart_type'] = y2_chart
                
        if field == 'values':
            values = col1.selectbox(
                label='Values',
                options=slq.metrics,
                placeholder='Select Value',
                key='chart_config_values',
            )
            chart_config['values'] = values
            
        if field == 'names':
            names = col1.selectbox(
                label='Select Dimension',
                options=slq.all_dimensions,
                key='chart_config_names',
            )
            chart_config['names'] = names
            
        if field == 'color':
            color = col1.selectbox(
                label='Color',
                options=[None] + slq.all_columns,
                placeholder='Select Color',
                key='chart_config_color',
            )
            chart_config['color'] = color
            
        if _can_add_field(selected_metrics, slq.metrics) and field == 'size':
            size = col1.selectbox(
                label='Size',
                options=[None] + _available_options(selected_metrics, slq.metrics),
                placeholder='Select Size',
                key='chart_config_size',
            )
            chart_config['size'] = size
            
        if _can_add_field(selected_dimensions, slq.all_dimensions) and field == 'facet_col':
            facet_col = col1.selectbox(
                label='Facet Column',
                options=[None] + _available_options(selected_dimensions, slq.all_dimensions),
                placeholder='Select Facet Column',
                key='chart_config_facet_col',
            )
            chart_config['facet_col'] = facet_col
            
        if _can_add_field(selected_dimensions, slq.all_dimensions) and field == 'facet_row':
            facet_row = col1.selectbox(
                label='Facet Row',
                options=[None] + _available_options(selected_dimensions, slq.all_dimensions),
                placeholder='Select Facet Row',
                key='chart_config_facet_row',
            )
            chart_config['facet_row'] = facet_row
            
        if field == 'histfunc':
            histfunc = col1.selectbox(
                label='Histogram Function',
                options=['sum', 'count', 'avg'],
                placeholder='Select Function',
                key='chart_config_histfunc',
            )
            chart_config['histfunc'] = histfunc
            
        if field == 'nbins':
            nbins = col1.number_input(
                label='Number of Bins',
                min_value=0,
                key='chart_config_nbins',
                value=0,
                help='If set to 0, the number of bins will be determined automatically',
            )
            chart_config['nbins'] = nbins
            
        # if field == 'trendline':
        #     trendline = col1.selectbox(
        #         label='Select Trendline',
        #         options=[None, 'ols'],
        #         key='chart_config_trendline',
        #     )
        #     chart_config['trendline'] = trendline
            
        if field == 'orientation':
            orientation = col1.selectbox(
                label='Select Orientation',
                options=['Vertical', 'Horizontal'],
                key='chart_config_orientation',
            )
            chart_config['orientation'] = orientation[:1].lower()
            if chart_config['orientation'] == 'h':
                x = chart_config.pop('x')
                y = chart_config.pop('y')
                chart_config['x'] = y
                chart_config['y'] = x
        
        if field == 'barmode' and len(slq.all_dimensions) > 1:
            barmode = col1.selectbox(
                label='Select Bar Mode',
                options=['group', 'stack'],
                key='chart_config_barmode',
            )
            chart_config['barmode'] = barmode
        
    with col2:
        df = _sort_dataframe(st.session_state.df, st.session_state.slq)
        y2_dict = chart_config.pop('y2', None)
        fig = getattr(px, selected_chart_type)(df, **chart_config)
        if y2_dict is not None and y2_dict['metric'] is not None:
            dct = {
                'y': y2_dict['metric'],
                'x': chart_config['x'],
                'chart_type': y2_dict['chart_type'],
            }
            fig = _add_secondary_yaxis(df, fig, dct)
        st.plotly_chart(fig, theme='streamlit', use_container_width=True)
