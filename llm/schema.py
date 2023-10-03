# stdlib
from enum import Enum
from typing import Any, Dict, List, Optional, Union

# third party
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
        "argument": "[GroupByInput!]!",
    },
    "where": {
        "kwarg": "$where",
        "argument": "[WhereInput!]!",
    },
    "orderBy": {
        "kwarg": "$orderBy",
        "argument": "[OrderByInput!]!",
    },
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
    def used_inputs(self) -> List[str]:
        inputs = []
        for key in GQL_MAP.keys():
            if getattr(self, key) is not None and len(getattr(self, key)) > 0:
                inputs.append(key)

        return inputs

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
                variables[input] = getattr(self, input).model_dump(exclude_none=True)
        return variables
