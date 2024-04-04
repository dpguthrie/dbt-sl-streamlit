# third party
import pandas as pd
import plotly.express as px
import streamlit as st

# first party
from schema import Query

CHART_TYPE_FIELDS = {
    "line": ["x", "y", "color", "facet_row", "facet_col", "y2"],
    "bar": ["x", "y", "color", "orientation", "barmode", "y2"],
    "pie": ["values", "names"],
    "area": ["x", "y", "color", "y2"],
    "scatter": ["x", "y", "color", "size", "facet_col", "facet_row", "trendline"],
    "histogram": ["x", "nbins", "histfunc"],
}


def _can_add_field(selections, available):
    return len(selections) < len(available)


def _available_options(selections, available):
    return [option for option in available if option not in selections]


def _sort_dataframe(df: pd.DataFrame, query: Query):
    try:
        time_dimensions = [
            col for col in df.columns if col in query.time_dimension_names
        ]
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
        "line": "Scatter",
        "bar": "Bar",
        "area": "Scatter",
    }

    new_fig = make_subplots(specs=[[{"secondary_y": True}]])

    # add traces from plotly express figure to first figure
    for t in fig.select_traces():
        new_fig.add_trace(t, secondary_y=False)

    addl_config = {}
    if dct["chart_type"] == "line":
        addl_config["mode"] = "lines"
    elif dct["chart_type"] == "area":
        addl_config["fill"] = "tozeroy"

    new_fig.add_trace(
        getattr(go, chart_map[dct["chart_type"]])(
            x=df[dct["x"]], y=df[dct["y"]], **addl_config
        ),
        secondary_y=True,
    )
    return new_fig


def create_chart(df, query: Query, suffix: str):
    col1, col2 = st.columns([0.2, 0.8])

    # Create default chart types
    if query.has_time_dimension:
        chart_types = ["line", "area", "bar"]
    elif query.has_multiple_metrics:
        chart_types = ["line", "scatter", "bar", "area"]
    else:
        chart_types = ["bar", "pie", "histogram", "scatter"]

    selected_chart_type = col1.selectbox(
        label="Select Chart Type",
        options=chart_types,
        key=f"selected_chart_type_{suffix}",
    )

    chart_config = {}

    for field in CHART_TYPE_FIELDS[selected_chart_type]:
        selected_dimensions = [
            col for col in chart_config.values() if col in query.dimension_names
        ]
        selected_metrics = [
            col for col in chart_config.values() if col in query.metric_names
        ]

        if field == "x":
            if selected_chart_type in ["scatter", "histogram"]:
                options = query.metric_names
            elif query.has_time_dimension:
                options = query.time_dimension_names
            else:
                options = query.dimension_names
            x = col1.selectbox(
                label="X-Axis",
                options=options,
                placeholder="Select Dimension",
                key=f"chart_config_x_{suffix}",
            )
            chart_config["x"] = x

        if field == "y":
            if len(query.metric_names) == 1 or selected_chart_type != "line":
                widget = "selectbox"
                y_kwargs = {}
            else:
                widget = "multiselect"
                y_kwargs = {"default": query.metric_names[0]}
            y = getattr(col1, widget)(
                label="Y-Axis",
                options=[
                    m for m in query.metric_names if m not in chart_config.values()
                ],
                placeholder="Select Metric",
                key=f"chart_config_y_{suffix}",
                **y_kwargs,
            )
            chart_config["y"] = y

        if (
            len(query.metric_names) > 1
            and field == "y2"
            and len([m for m in query.metric_names if m not in chart_config.values()])
            > 0
        ):
            chart_config["y2"] = {}
            expander = col1.expander("Secondary Axis Options")
            y2 = expander.selectbox(
                label="Secondary Axis",
                options=[None]
                + [m for m in query.metric_names if m not in chart_config.values()],
                key=f"chart_config_y2_{suffix}",
            )
            chart_config["y2"]["metric"] = y2
            y2_chart = expander.selectbox(
                label="Secondary Axis Chart Type",
                options=chart_types,
                index=chart_types.index(selected_chart_type),
                key=f"chart_config_y2_chart_type_{suffix}",
            )
            chart_config["y2"]["chart_type"] = y2_chart

        if field == "values":
            values = col1.selectbox(
                label="Values",
                options=query.metric_names,
                placeholder="Select Value",
                key=f"chart_config_values_{suffix}",
            )
            chart_config["values"] = values

        if field == "names":
            names = col1.selectbox(
                label="Select Dimension",
                options=query.dimension_names,
                key=f"chart_config_names_{suffix}",
            )
            chart_config["names"] = names

        if field == "color":
            color = col1.selectbox(
                label="Color",
                options=[None] + query.all_names,
                placeholder="Select Color",
                key=f"chart_config_color_{suffix}",
            )
            chart_config["color"] = color

        if _can_add_field(selected_metrics, query.metric_names) and field == "size":
            size = col1.selectbox(
                label="Size",
                options=[None]
                + _available_options(selected_metrics, query.metric_names),
                placeholder="Select Size",
                key=f"chart_config_size_{suffix}",
            )
            chart_config["size"] = size

        if (
            _can_add_field(selected_dimensions, query.dimension_names)
            and field == "facet_col"
        ):
            facet_col = col1.selectbox(
                label="Facet Column",
                options=[None]
                + _available_options(selected_dimensions, query.dimension_names),
                placeholder="Select Facet Column",
                key=f"chart_config_facet_col_{suffix}",
            )
            chart_config["facet_col"] = facet_col

        if (
            _can_add_field(selected_dimensions, query.dimension_names)
            and field == "facet_row"
        ):
            facet_row = col1.selectbox(
                label="Facet Row",
                options=[None]
                + _available_options(selected_dimensions, query.dimension_names),
                placeholder="Select Facet Row",
                key=f"chart_config_facet_row_{suffix}",
            )
            chart_config["facet_row"] = facet_row

        if field == "histfunc":
            histfunc = col1.selectbox(
                label="Histogram Function",
                options=["sum", "count", "avg"],
                placeholder="Select Function",
                key=f"chart_config_histfunc_{suffix}",
            )
            chart_config["histfunc"] = histfunc

        if field == "nbins":
            nbins = col1.number_input(
                label="Number of Bins",
                min_value=0,
                key=f"chart_config_nbins_{suffix}",
                value=0,
                help="If set to 0, the number of bins will be determined automatically",
            )
            chart_config["nbins"] = nbins

        # if field == 'trendline':
        #     trendline = col1.selectbox(
        #         label='Select Trendline',
        #         options=[None, 'ols'],
        #         key='chart_config_trendline',
        #     )
        #     chart_config['trendline'] = trendline

        if field == "orientation":
            orientation = col1.selectbox(
                label="Select Orientation",
                options=["Vertical", "Horizontal"],
                key=f"chart_config_orientation_{suffix}",
            )
            chart_config["orientation"] = orientation[:1].lower()
            if chart_config["orientation"] == "h":
                x = chart_config.pop("x")
                y = chart_config.pop("y")
                chart_config["x"] = y
                chart_config["y"] = x

        if field == "barmode" and len(query.dimension_names) > 1:
            barmode = col1.selectbox(
                label="Select Bar Mode",
                options=["group", "stack"],
                key=f"chart_config_barmode_{suffix}",
            )
            chart_config["barmode"] = barmode

    st.session_state.chart_config = chart_config
    with col2:
        df = _sort_dataframe(df, query)
        y2_dict = chart_config.pop("y2", None)
        fig = getattr(px, selected_chart_type)(df, **chart_config)
        if y2_dict is not None and y2_dict["metric"] is not None:
            dct = {
                "y": y2_dict["metric"],
                "x": chart_config["x"],
                "chart_type": y2_dict["chart_type"],
            }
            fig = _add_secondary_yaxis(df, fig, dct)
        st.plotly_chart(fig, theme="streamlit", use_container_width=True)
