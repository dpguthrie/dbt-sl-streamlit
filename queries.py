GRAPHQL_QUERIES = {
    'metrics': '''
query GetMetrics($environmentId: BigInt!) {
  metrics(environmentId: $environmentId) {
    description
    name
    queryableGranularities
    type
    dimensions {
      description
      name
      type
    }
  }
}
    ''',
    'dimensions': '''
query GetDimensions($environmentId: BigInt!, $metrics: [String!]!) {
  dimensions(environmentId: $environmentId, metrics: $metrics) {
    description
    expr
    isPartition
    metadata {
      fileSlice {
        content
        endLineNumber
        filename
        startLineNumber
      }
      repoFilePath
    }
    name
    qualifiedName
    type
    typeParams {
      timeGranularity
      validityParams {
        isEnd
        isStart
      }
    }
  }
}
    ''',
    'dimension_values': '''
mutation GetDimensionValues($environmentId: BigInt!, $groupBy: [String!]!, $metrics: [String!]!) {
  createDimensionValuesQuery(
    environmentId: $environmentId
    groupBy: $groupBy
    metrics: $metrics
  ) {
    queryId
  }
}
    ''',
    'metric_for_dimensions': '''
query GetMetricsForDimensions($environmentId: BigInt!, $dimensions: [String!]!) {
  metricsForDimensions(environmentId: $environmentId, dimensions: $dimensions) {
    description
    name
    queryableGranularities
    type
  }
}
    ''',
    'create_query': '''
mutation CreateQuery({arguments}) {{
  createQuery(
    {kwargs}
  ) {{
    queryId
  }}
}}
    ''',
    'get_results': '''
query GetResults($environmentId: BigInt!, $queryId: String!) {
  query(environmentId: $environmentId, queryId: $queryId) {
    arrowResult
    error
    queryId
    sql
    status
  }
}
    ''',
    'queryable_granularities': '''
query GetQueryableGranularities($environmentId: BigInt!, $metrics:[String!]!) {
  queryableGranularities(environmentId: $environmentId, metrics: $metrics)
}
    ''',
    'metrics_for_dimensions': '''
query GetMetricsForDimensions($environmentId: BigInt!, $dimensions:[String!]!) {
  metricsForDimensions(environmentId: $environmentId, dimensions: $dimensions) {
    description
    name
    queryableGranularities
    type
    filter {
      whereSqlTemplate
    }
  }
}
    '''
}

JDBC_QUERIES = {
    'metrics': '''
select *
from {{
    semantic_layer.metrics()
}}
''',
    'dimensions': '''
select *
from {{{{
    semantic_layer.dimensions(
        metrics={metrics}
    )
}}}}
''',
    'dimension_values': '''
select *
from {{{{
    semantic_layer.dimension_values(
        metrics={metrics},
        group_by='{dimension}'
    )
}}}}
''',
    'queryable_granularities': '''
select *
from {{{{
    semantic_layer.queryable_granularities(
        metrics={metrics}
    )
}}}}
''',
    'metrics_for_dimensions': '''
select *
from {{{{
    semantic_layer.metrics_for_dimensions(
        group_by={dimensions}
    )
}}}}
'''
}
