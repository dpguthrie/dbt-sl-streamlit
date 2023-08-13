queries = {
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
