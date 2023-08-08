# stdlib
from typing import Dict, List

# first party
from helpers import keys_exist_in_dict


class SemanticLayerQuery:
    def __init__(self, state: Dict):
        self.state = state
        self._time_dimensions = []
        self._format_metrics()
        self._format_dimensions()
        self._format_filters()
        self._format_order_by()
        
    def _is_time_dimension(self, dimension: str):
        try:
            dim_type = self.state.dimension_dict[dimension]['type']
        except KeyError:
            return False
        
        is_time_dimension = dim_type.lower() == 'time'
        if is_time_dimension:
            dim = f'{dimension}__{self.state.selected_grain}'
            if dim not in self._time_dimensions:
                self._time_dimensions.append(dim)
        return is_time_dimension
        
    def _format_metrics(self) -> None:
        self.metrics = self.state.selected_metrics
    
    def _format_dimensions(self) -> None:
        formatted_dimensions = []
        for dim in self.state.selected_dimensions:
            if self._is_time_dimension(dim):
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
            if self._is_time_dimension(column):
                dim_class = f"TimeDimension('{column}', '{self.state.selected_grain.upper()}')"
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
            if self._is_time_dimension(column):
                column = f'{column}__{self.state.selected_grain}'
            if direction.lower() == 'desc':
                formatted_orders.append(f'-{column}')
            else:
                formatted_orders.append(column)
        self._order_by = formatted_orders
        
    @property
    def _query_inner(self):
        text = f'metrics={self.metrics}'
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
