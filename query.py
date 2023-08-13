# stdlib
from typing import Dict, List

# first party
from helpers import keys_exist_in_dict


class SemanticLayerQuery:
    def __init__(self, state: Dict):
        self.state = state
        self._dimensions = {}
        self._classify_dimensions()
        self._format_metrics()
        self._format_dimensions()
        self._format_filters()
        self._format_order_by()
        
    @property
    def has_time_dimension(self):
        return 'time' in self._dimensions.keys()
    
    @property
    def has_entity_dimension(self):
        return 'entity' in self._dimensions.keys()
    
    @property
    def has_categorical_dimension(self):
        return 'categorical' in self._dimensions.keys()
    
    def _is_dim_type(self, dimension_type, dimension):
        try:
            return dimension_type.lower() == self.state.dimension_dict[dimension]['type'].lower()
        except KeyError:
            return False
        
    def _classify_dimensions(self):
        for dimension in self.state.selected_dimensions:
            try:
                dim_type = self.state.dimension_dict[dimension]['type'].lower()
            except KeyError:
                pass
            else:
                if dim_type not in self._dimensions:
                    self._dimensions[dim_type] = []
                if dim_type == 'time':
                    dimension = f'{dimension}__{self.state.selected_grain}'
                self._dimensions[dim_type].append(dimension)
        
    def _format_metrics(self) -> None:
        self._metrics = self.state.selected_metrics
    
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
        for column, operator, condition in filters:
            if self._is_dim_type('time', column):
                dim_class = f"TimeDimension('{column}', '{self.state.get('selected_grain', 'day').upper()}')"
            elif self._is_dim_type('entity', column):
                dim_class = f"Entity('{column}')"
            else:
                dim_class = f"Dimension('{column}')"
            formatted_filters.append(
                f"{{{{ {dim_class} }}}} {operator} {condition}"
            )
        self._where = ' AND '.join(formatted_filters)
        
    def _format_order_by(self) -> None:
        orders = self._create_list_of_lists('order', ['column', 'direction'])
        formatted_orders = []
        for column, direction in orders:
            if self._is_dim_type('time', column):
                column = f'{column}__{self.state.selected_grain}'
            if direction.lower() == 'desc':
                formatted_orders.append(f'-{column}')
            else:
                formatted_orders.append(column)
        self._order_by = formatted_orders
        
    @property
    def _query_inner(self):
        text = f'metrics={self._metrics}'
        if len(self._group_by) > 0:
            text += f',\n        group_by={self._group_by}'
        if len(self._where) > 0:
            text += f',\n        where="{self._where}"'
        if len(self._order_by) > 0:
            text += f',\n        order_by={self._order_by}'
        if self.state.selected_limit is not None and self.state.selected_limit != 0:
            text += f',\n        limit={self.state.selected_limit}'
        if self.state.selected_explain:
            text += f',\n        explain={self.state.selected_explain}'
        return text
        
    @property
    def query(self):
        sql = f'''
select *
from {{{{
    semantic_layer.query(
        {self._query_inner}
    )
}}}}
        '''
        return sql
