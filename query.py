# stdlib
import json
from itertools import chain
from typing import Dict, List

# first party
from helpers import keys_exist_in_dict
from queries import GRAPHQL_QUERIES


class SemanticLayerQuery:

    def __init__(self, state: Dict):
        self.state = state
        self._classify_dimensions()
        self._format_metrics()
        self._format_dimensions()
        self._format_filters()
        self._format_order_by()
        self._create_valid_options()
        
    def _has_type_dimension(self, dim_type: str):
        return dim_type in self.dimensions.keys()
        
    @property
    def has_time_dimension(self):
        return self._has_type_dimension('time')
    
    @property
    def has_entity_dimension(self):
        return self._has_type_dimension('entity')
    
    @property
    def has_categorical_dimension(self):
        return self._has_type_dimension('categorical')
    
    @property
    def all_dimensions(self):
        return list(chain.from_iterable(self.dimensions.values()))
    
    @property
    def dimensions_x_time(self):
        return self.dimensions['entity'] + self.dimensions['categorical']
    
    @property
    def all_columns(self):
        return self.all_dimensions + self.metrics
    
    def _is_dim_type(self, dimension_type, dimension):
        try:
            return dimension_type.lower() == self.state.dimension_dict[dimension]['type'].lower()
        except KeyError:
            return False
        
    def _classify_dimensions(self):
        self.dimensions = {}
        for dimension in self.state.selected_dimensions:
            try:
                dim_type = self.state.dimension_dict[dimension]['type'].lower()
            except KeyError:
                pass
            else:
                if dim_type not in self.dimensions:
                    self.dimensions[dim_type] = []
                if dim_type == 'time':
                    dimension = f'{dimension}__{self.state.selected_grain}'
                self.dimensions[dim_type].append(dimension)
        
    def _format_metrics(self) -> None:
        self.metrics = self.state.selected_metrics
    
    def _format_dimensions(self) -> None:
        formatted_dimensions = []
        for dim in self.state.selected_dimensions:
            if self._is_dim_type('time', dim):
                formatted_dimensions.append(
                    f'{dim}__{self.state.selected_grain}'
                )
            else:
                formatted_dimensions.append(dim)
        self._group_by = formatted_dimensions

    def _create_list_of_lists(self, sql_type: str, components: List[str]):
        results = []
        for i in range(10):
            keys = [f'{sql_type}_{component}_{i}' for component in components]
            if keys_exist_in_dict(keys, self.state):
                results.append([self.state[key] for key in keys])
            else:
                break
        return results
        
    def _format_filters(self) -> None:
        filters = self._create_list_of_lists('where', ['column', 'operator', 'condition'])
        formatted_filters = []
        gql_filters = []
        for column, operator, condition in filters:
            if self._is_dim_type('time', column):
                dim_class = f"TimeDimension('{column}', '{self.state.get('selected_grain', 'day').upper()}')"
            elif self._is_dim_type('entity', column):
                dim_class = f"Entity('{column}')"
            else:
                dim_class = f"Dimension('{column}')"
            where_string = f"{{{{ {dim_class} }}}} {operator} {condition}"
            formatted_filters.append(where_string)
            gql_filters.append({'sql': where_string})
        self._where = ' AND '.join(formatted_filters)
        self._where_kwargs = gql_filters
        
    def _format_order_by(self) -> None:
        def _format_order_by_kwarg(column: str, direction: str) -> Dict:
            dct = {}
            if column in self.metrics:
                dct['metric'] = {'name': column}
            else:
                if self._is_dim_type('time', column):
                    column, grain = column.split('__')
                    dct['groupBy'] = {'name': column}
                    dct['groupBy']['grain'] = grain
                else:
                    dct['groupBy'] = {'name': column}
            if direction.lower() == 'desc':
                dct['descending'] = True
            return dct

        orders = self._create_list_of_lists('order', ['column', 'direction'])
        formatted_orders = []
        gql_orders = []
        for column, direction in orders:
            if self._is_dim_type('time', column):
                column = f'{column}__{self.state.selected_grain}'
            if direction.lower() == 'desc':
                formatted_orders.append(f'-{column}')
            else:
                formatted_orders.append(column)
            gql_orders.append(_format_order_by_kwarg(column, direction))
        self._order_by = formatted_orders
        self._order_by_kwargs = gql_orders
        
    def _create_valid_options(self):
        text = f'metrics={self.metrics}'
        gql = {
            'arguments': {'$environmentId': 'BigInt!', '$metrics': '[MetricInput!]!'},
            'kwargs': {'environmentId': '$environmentId', 'metrics': '$metrics'},
            'variables': {'metrics': [{'name': m} for m in self.metrics]},
        }
        if len(self._group_by) > 0:
            text += f',\n        group_by={self._group_by}'
            gql['arguments']['$groupBy'] = '[GroupByInput!]!'
            gql['kwargs']['groupBy'] = '$groupBy'
            gql['variables']['groupBy'] = []
            for group in self._group_by:
                parts = group.split('__')
                if len(parts) > 1 and self._is_dim_type('time', parts[0]):
                    group_variable = {'name': parts[0], 'grain': parts[1].upper()}
                else:
                    group_variable = {'name': group}
                gql['variables']['groupBy'].append(group_variable)
        if len(self._where) > 0:
            text += f',\n        where="{self._where}"'
            gql['arguments']['$where'] = '[WhereInput!]!'
            gql['kwargs']['where'] = '$where'
            gql['variables']['where'] = self._where_kwargs
        if len(self._order_by) > 0:
            text += f',\n        order_by={self._order_by}'
            gql['arguments']['$orderBy'] = '[OrderByInput!]!'
            gql['kwargs']['orderBy'] = '$orderBy'
            gql['variables']['orderBy'] = self._order_by_kwargs
        if self.state.selected_limit is not None and self.state.selected_limit != 0:
            text += f',\n        limit={self.state.selected_limit}'
            gql['kwargs']['limit'] = self.state.selected_limit
        self._jdbc_text = text
        self._gql = gql
        
    @property
    def jdbc_query(self):
        sql = f'''
select *
from {{{{
    semantic_layer.query(
        {self._jdbc_text}
    )
}}}}
        '''
        return sql

    @property
    def graphql_query(self):
        return GRAPHQL_QUERIES['create_query'].format(**{
            'arguments': ', '.join(f'{k}: {v}' for k, v in self._gql['arguments'].items()),
            'kwargs': ',\n    '.join([
                f'{k}: {v}' for k, v in self._gql['kwargs'].items()
            ])
        })
