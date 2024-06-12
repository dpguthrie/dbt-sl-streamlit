# third party
from dbtc import dbtCloudClient
from langchain.schema.document import Document

# first party
from client import ConnAttr


def _dict_to_list(d, metadata_type: str):
    docs = []
    for k, v in d.items():
        v["metrics"] = ", ".join([m for m in v["metrics"]])
        docs.append(
            Document(
                page_content=f"{metadata_type} name: {k}; {metadata_type} description: {v['description']}",
                metadata={k: v for k, v in v.items() if v},
            )
        )
    return docs


def get_metadata(conn: ConnAttr):
    """Get metadata about a user's semantic layer."""
    documents = []
    all_metrics = []
    dimensions = {}
    entities = {}
    response = dbtCloudClient(
        service_token=conn.auth_header.split(" ")[-1],
        environment_id=conn.params["environmentid"],
    ).sl.list_metrics()
    if not response["data"]:
        raise ValueError("No metrics found for the input JDBC url.")

    metrics = response["data"]["metrics"]
    for m in metrics:
        all_metrics.append(m["name"])
        for d in m["dimensions"]:
            dim_name = d["name"]
            if dim_name not in dimensions:
                dimensions[dim_name] = {
                    "metrics": [],
                    "description": d["description"],
                    "expr": d["expr"],
                    "label": d["label"],
                    "qualified_name": d["qualifiedName"],
                    "dimension_type": d["type"],
                }
            dimensions[dim_name]["metrics"].append(m["name"])
        for e in m["entities"]:
            ent_name = e["name"]
            if ent_name not in entities:
                entities[ent_name] = {
                    "metrics": [],
                    "description": e["description"],
                    "expr": e["expr"],
                    "key_type": e["type"],
                }
            entities[ent_name]["metrics"].append(m["name"])
        documents.append(
            Document(
                page_content=f"metric name: {m['name']}; metric description: {m['description']}",
                metadata={
                    "metric_type": m["type"],
                    "metric_label": m["label"],
                    "queryable_granularities": ", ".join(
                        [g for g in m["queryableGranularities"]]
                    ),
                    "dimensions": ", ".join([d["name"] for d in m["dimensions"]]),
                    "entities": ", ".join([d["name"] for d in m["entities"]]),
                    "measures": ", ".join(
                        [
                            f"measure name: {m['name']}; sql expr: {m['expr']}; aggregation: {m['agg']}"
                            for m in m["measures"]
                        ]
                    ),
                    "requires_metric_time": m["requiresMetricTime"],
                },
            )
        )
    documents.append(
        Document(
            page_content=f"All available metrics: {', '.join(all_metrics)}",
        )
    )
    return (
        documents
        + _dict_to_list(dimensions, "dimension")
        + _dict_to_list(entities, "entity")
    )
