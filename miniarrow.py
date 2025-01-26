import pyarrow as pa
import pyarrow.compute as pc


class MiniQueryEngine:
    """
    A lightweight query engine based on Apache Arrow.

    Provides functionality for creating tables, filtering, aggregation,
    joins, sorting, and group-by operations.
    """

    def __init__(self):
        """
        Initialize the query engine with an empty table collection.
        """
        self.tables = {}

    def create_table(self, name, data):
        """
        Create a table from a dictionary of lists.

        Args:
            name (str): Name of the table.
            data (dict): Dictionary where keys are column names and values are lists.
        """
        self.tables[name] = pa.Table.from_pydict(data)

    def filter_table(self, table_name, column_name, condition, value):
        """
        Filter a table based on a condition and value.

        Args:
            table_name (str): Name of the table.
            column_name (str): Column to filter.
            condition (str): Filter condition ('==', '>', '<', '>=', '<=', '!=').
            value: Value to compare.

        Returns:
            dict: Filtered table as a dictionary of lists.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        column = table[column_name]

        conditions = {
            "==": pc.equal,
            ">": pc.greater,
            "<": pc.less,
            ">=": pc.greater_equal,
            "<=": pc.less_equal,
            "!=": pc.not_equal,
        }
        if condition not in conditions:
            raise ValueError(
                f"Unsupported condition. Use one of: {list(conditions.keys())}"
            )

        mask = conditions[condition](column, value)
        filtered_table = table.filter(mask)
        return filtered_table.to_pydict()

    def aggregate_table(self, table_name, column_name, agg_func):
        """
        Aggregate a column in a table.

        Args:
            table_name (str): Name of the table.
            column_name (str): Column to aggregate.
            agg_func (str): Aggregation function ('sum', 'mean', 'min', 'max', 'count').

        Returns:
            Result of the aggregation.
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
            "count": lambda col: pa.scalar(len(col)),
        }
        if agg_func not in functions:
            raise ValueError(
                f"Unsupported aggregation function. Use one of: {list(functions.keys())}"
            )

        result = functions[agg_func](column)
        return result.as_py()

    def join_tables(self, left_table, right_table, left_key, right_key, join_type="inner"):
        """
        Perform a join between two tables.

        Args:
            left_table (str): Name of the left table.
            right_table (str): Name of the right table.
            left_key (str): Key column in the left table.
            right_key (str): Key column in the right table.
            join_type (str): Type of join ('inner', 'left', 'right', 'full').

        Returns:
            dict: Resulting table as a dictionary of lists.
        """
        if left_table not in self.tables or right_table not in self.tables:
            raise ValueError("One or both tables do not exist.")

        left = self.tables[left_table]
        right = self.tables[right_table]

        supported_join_types = ["inner", "left", "right", "full"]
        if join_type not in supported_join_types:
            raise ValueError(f"Join type must be one of: {supported_join_types}")

        result = left.join(right, left_key, right_key, join_type=join_type)
        return result.to_pydict()

    def sort_table(self, table_name, column_name, ascending=True):
        """
        Sort a table based on a specified column.

        Args:
            table_name (str): Name of the table.
            column_name (str): Column to sort.
            ascending (bool): Sort order (default is True).

        Returns:
            dict: Sorted table as a dictionary of lists.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        sorted_table = table.sort_by([(column_name, "ascending" if ascending else "descending")])
        return sorted_table.to_pydict()

    def group_by(self, table_name, group_column, agg_column, agg_func):
        """
        Group by a column and apply an aggregation function.

        Args:
            table_name (str): Name of the table.
            group_column (str): Column to group by.
            agg_column (str): Column to aggregate.
            agg_func (str): Aggregation function ('sum', 'mean', 'min', 'max', 'count').

        Returns:
            dict: Grouped and aggregated table as a dictionary of lists.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        if agg_func not in ["sum", "mean", "min", "max", "count"]:
            raise ValueError("Unsupported aggregation function.")

        grouped_table = table.group_by([group_column]).aggregate([(agg_func, agg_column)])
        return grouped_table.to_pydict()


# Example Usage
if __name__ == "__main__":
    engine = MiniQueryEngine()

    engine.create_table("users", {
        "user_id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [25, 30, 35, 40]
    })

    # Example operations
    filtered = engine.filter_table("users", "age", ">", 30)
    print(filtered)
