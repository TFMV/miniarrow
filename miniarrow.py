import pyarrow as pa
import pyarrow.compute as pc

class MiniQueryEngine:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, data):
        """
        Create a table from a dictionary of lists.
        """
        self.tables[name] = pa.Table.from_pydict(data)

    def filter_table(self, table_name, column_name, condition, value):
        """
        Filter a table based on a condition and value.
        Supported conditions: '==', '>', '<', '>=', '<=', '!='.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        column = table[column_name]

        # Create a filter expression
        conditions = {
            "==": pc.equal,
            ">": pc.greater,
            "<": pc.less,
            ">=": pc.greater_equal,
            "<=": pc.less_equal,
            "!=": pc.not_equal
        }
        if condition not in conditions:
            raise ValueError(f"Unsupported condition. Use one of: {list(conditions.keys())}")

        mask = conditions[condition](column, value)
        filtered_table = table.filter(mask)
        return filtered_table.to_pydict()

    def aggregate_table(self, table_name, column_name, agg_func):
        """
        Aggregate a column in a table.
        Supported functions: 'sum', 'mean', 'min', 'max', 'count'.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        column = table[column_name]

        functions = {
            "sum": pc.sum,
            "mean": pc.mean,
            "min": pc.min,
            "max": pc.max,
            "count": lambda col: pa.array([len(col)])
        }
        if agg_func not in functions:
            raise ValueError(f"Unsupported aggregation function. Use one of: {list(functions.keys())}")

        result = functions[agg_func](column)
        return result.as_py() if isinstance(result, pa.Scalar) else result.to_pylist()[0]

    def join_tables(self, left_table, right_table, left_key, right_key, join_type='inner'):
        """
        Perform a join between two tables.
        Supported join types: 'inner', 'left', 'right', 'full', 'left_semi', 'right_semi'.
        """
        if left_table not in self.tables or right_table not in self.tables:
            raise ValueError("One or both tables do not exist.")

        left = self.tables[left_table]
        right = self.tables[right_table]

        supported_join_types = ['inner', 'left', 'right', 'full', 'left_semi', 'right_semi']
        if join_type not in supported_join_types:
            raise ValueError(f"Join type must be one of: {supported_join_types}")

        result = left.join(right, left_key, right_key, join_type=join_type)
        return result.to_pydict()

    def sort_table(self, table_name, column_name, ascending=True):
        """
        Sort a table based on a specified column.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        sorted_table = table.sort_by([(column_name, "ascending" if ascending else "descending")])
        return sorted_table.to_pydict()

    def group_by(self, table_name, group_column, agg_column, agg_func):
        """
        Group by a column and apply an aggregation function.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        if agg_func not in ["sum", "mean", "min", "max", "count"]:
            raise ValueError("Unsupported aggregation function. Use one of: sum, mean, min, max, count.")

        grouped_table = table.group_by([group_column]).aggregate(
            [(agg_func, agg_column)]
        )
        return grouped_table.to_pydict()
