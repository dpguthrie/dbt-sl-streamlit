# stdlib
from enum import Enum
from typing import Any, Dict, List, Optional, Union

# third party
import streamlit as st
from pydantic import BaseModel, Field, model_validator

# first party
from queries import GRAPHQL_QUERIES


GQL_MAP: Dict = {
    "metrics": {
        "kwarg": "$metrics",
        "argument": "[MetricInput!]!",
    },
    "groupBy": {
        "kwarg": "$groupBy",
        "argument": "[GroupByInput!]",
    },
    "where": {
        "kwarg": "$where",
        "argument": "[WhereInput!]",
    },
    "orderBy": {
        "kwarg": "$orderBy",
        "argument": "[OrderByInput!]",
    },
    "limit": {"kwarg": "$limit", "argument": "Int"},
}


class TimeGranularity(str, Enum):
    day = "DAY"
    week = "WEEK"
    month = "MONTH"
    quarter = "QUARTER"
    year = "YEAR"


class MetricInput(BaseModel):
    name: str = Field(
        description=(
            "Metric name defined by the user.  A metric can generally be thought of "
            "as a descriptive statistic, indicator, or figure of merit used to "
            "describe or measure something quantitatively."
        )
    )


class GroupByInput(BaseModel):
    name: str = Field(
        description=(
            "Dimension name defined by the user.  They often contain qualitative "
            "values (such as names, dates, or geographical data). You can use "
            "dimensions to categorize, segment, and reveal the details in your data. "
            "A common dimension used here will be metric_time.  This will ALWAYS have "
            "an associated grain."
        )
    )
    grain: Optional[TimeGranularity] = Field(
        default=None,
        description=(
            "The grain is the time interval represented by a single point in the data"
        ),
    )

    class Config:
        use_enum_values = True


class OrderByInput(BaseModel):
    metric: Optional[MetricInput] = None
    groupBy: Optional[GroupByInput] = None
    descending: Optional[bool] = None

    @model_validator(mode="before")
    def check_metric_or_groupBy(cls, values):
        if (values.get("metric") is None) and (values.get("groupBy") is None):
            raise ValueError("either metric or groupBy is required")
        if (values.get("metric") is not None) and (values.get("groupBy") is not None):
            raise ValueError("only one of metric or groupBy is allowed")
        return values

    class Config:
        exclude_none = True


class WhereInput(BaseModel):
    sql: str


class Query(BaseModel):
    metrics: List[MetricInput]
    groupBy: Optional[List[GroupByInput]] = None
    where: Optional[List[WhereInput]] = None
    orderBy: Optional[List[OrderByInput]] = None
    limit: Optional[int] = None

    @property
    def all_names(self):
        return self.metric_names + self.dimension_names

    @property
    def metric_names(self):
        return [m.name for m in self.metrics]

    @property
    def dimension_names(self):
        return [
            f"{g.name}__{g.grain.lower()}" if g.grain is not None else g.name
            for g in self.groupBy or []
        ]

    @property
    def time_dimension_names(self):
        return [
            f"{g.name}__{g.grain.lower()}"
            for g in self.groupBy or []
            if g.grain is not None
        ]

    @property
    def has_time_dimension(self):
        if self.groupBy is not None:
            return any([g.grain is not None for g in self.groupBy])

        return False

    @property
    def has_multiple_metrics(self):
        return len(self.metrics) > 1

    @property
    def used_inputs(self) -> List[str]:
        inputs = []
        for key in GQL_MAP.keys():
            prop = getattr(self, key)
            if prop is not None:
                try:
                    if len(prop) > 0:
                        inputs.append(key)
                except TypeError:
                    inputs.append(key)

        return inputs

    @property
    def _jdbc_text(self) -> str:
        text = f"metrics={[m.name for m in self.metrics]}"
        if self.groupBy is not None:
            group_by = [
                f"{g.name}__{g.grain.lower()}" if g.grain is not None else g.name
                for g in self.groupBy
            ]
            text += f",\n        group_by={group_by}"
        if self.where is not None:
            where = " AND ".join([w.sql for w in self.where])
            text += f',\n        where="{where}"'
        if self.orderBy is not None:
            names = []
            for order in self.orderBy:
                obj = order.metric if order.metric else order.groupBy
                if hasattr(obj, "grain") and obj.grain is not None:
                    name = f"{obj.name}__{obj.grain.lower()}"
                else:
                    name = obj.name
                if order.descending:
                    name = f"-{name}"
                names.append(name)
            text += f",\n        order_by={names}"
        if self.limit is not None:
            text += f",\n        limit={self.limit}"
        return text

    @property
    def jdbc_query(self):
        sql = f"""
select *
from {{{{
    semantic_layer.query(
        {self._jdbc_text}
    )
}}}}
        """
        return sql

    @property
    def gql(self) -> str:
        query = GRAPHQL_QUERIES["create_query"]
        kwargs = {"environmentId": "$environmentId"}
        arguments = {"environmentId": "BigInt!"}
        for input in self.used_inputs:
            kwargs[input] = GQL_MAP[input]["kwarg"]
            arguments[input] = GQL_MAP[input]["argument"]
        return query.format(
            **{
                "arguments": ", ".join(f"${k}: {v}" for k, v in arguments.items()),
                "kwargs": ",\n    ".join([f"{k}: {v}" for k, v in kwargs.items()]),
            }
        )

    @property
    def variables(self) -> Dict[str, List[Any]]:
        variables = {}
        for input in self.used_inputs:
            data = getattr(self, input)
            if isinstance(data, list):
                variables[input] = [m.model_dump(exclude_none=True) for m in data]
            else:
                try:
                    variables[input] = getattr(self, input).model_dump(
                        exclude_none=True
                    )
                except AttributeError:
                    variables[input] = getattr(self, input)
        return variables


class QueryLoader:
    def __init__(self, state: st.session_state):
        self.state = state

    def create(self):
        return Query(
            metrics=self._metrics,
            groupBy=self._groupBy or None,
            where=self._where or None,
            orderBy=self._orderBy or None,
            limit=self._limit or None,
        )

    def _is_time_dimension(self, dimension: str):
        return self.state.dimension_dict[dimension]["type"].lower() == "time"

    @property
    def _metrics(self):
        return [MetricInput(name=m) for m in self.state.selected_metrics]

    @property
    def _groupBy(self):
        dimensions = []
        for dimension in self.state.selected_dimensions:
            kwargs = {"name": dimension}
            if self._is_time_dimension(dimension):
                kwargs["grain"] = self.state.selected_grain.upper()
            dimensions.append(GroupByInput(**kwargs))
        return dimensions

    @property
    def _where(self):
        def where_dimension(dimension: str):
            if self._is_time_dimension(dimension):
                return f"TimeDimension('{dimension}', '{self.state.get('selected_grain', 'day').upper()}')"

            return f"Dimension('{dimension}')"

        wheres = []
        for i in range(10):
            column = f"where_column_{i}"
            operator = f"where_operator_{i}"
            condition = f"where_condition_{i}"
            if column in self.state and self.state[column] is not None:
                wheres.append(
                    WhereInput(
                        sql=f"{{{{ {where_dimension(self.state[column])} }}}} {self.state[operator]} {self.state[condition]}"
                    )
                )
            else:
                break
        return wheres

    @property
    def _orderBy(self):
        def metric(metric_name):
            return {"metric": {"name": metric_name}}

        def groupBy(dimension_name):
            dct = {"name": dimension_name}
            if self._is_time_dimension(dimension_name):
                dct["grain"] = self.state.selected_grain.upper()

            return {"groupBy": dct}

        orderBys = []
        for i in range(10):
            column = f"order_column_{i}"
            direction = f"order_direction_{i}"
            if column in self.state and self.state[column] is not None:
                name = self.state[column]
                if name in self.state.selected_metrics:
                    dct = metric(name)
                else:
                    dct = groupBy(name)
                if self.state[direction].lower() == "desc":
                    dct["descending"] = True
                orderBys.append(OrderByInput(**dct))
            else:
                break
        return orderBys

    @property
    def _limit(self):
        if self.state.selected_limit is not None and self.state.selected_limit != 0:
            return self.state.selected_limit

        return None
