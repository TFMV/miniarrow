import pyarrow as pa
import pyarrow.compute as pc
import time


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

        Raises:
            ValueError: If name is empty or data is not a dictionary.
            TypeError: If data values are not lists or have inconsistent lengths.
        """
        if not name or not isinstance(name, str):
            raise ValueError("Table name must be a non-empty string")
        if not isinstance(data, dict) or not data:
            raise ValueError("Data must be a non-empty dictionary")

        # Validate data structure
        lengths = set(len(col) for col in data.values())
        if len(lengths) > 1:
            raise TypeError("All columns must have the same length")

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

        Raises:
            ValueError: If table doesn't exist or column not found.
            TypeError: If value type doesn't match column type.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        if column_name not in table.column_names:
            raise ValueError(
                f"Column '{column_name}' not found in table '{table_name}'"
            )

        column = table[column_name]

        # Validate value type matches column type
        try:
            pa.array([value], type=column.type)
        except (TypeError, pa.ArrowInvalid):
            raise TypeError(
                f"Value type {type(value)} doesn't match column type {column.type}"
            )

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

        Raises:
            ValueError: If table/column doesn't exist or invalid aggregation function.
            TypeError: If column type doesn't support the aggregation function.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        if column_name not in table.column_names:
            raise ValueError(
                f"Column '{column_name}' not found in table '{table_name}'"
            )

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

        # Validate aggregation function compatibility with column type
        if agg_func in ["sum", "mean"]:
            if not (
                pa.types.is_integer(column.type) or pa.types.is_floating(column.type)
            ):
                raise TypeError(
                    f"Aggregation '{agg_func}' requires numeric column type"
                )

        try:
            result = functions[agg_func](column)
            return result.as_py()
        except pa.ArrowInvalid as e:
            raise TypeError(f"Aggregation failed: {str(e)}")

    def join_tables(
        self, left_table, right_table, left_keys, right_keys, join_type="inner"
    ):
        """
        Perform a join between two tables with support for multi-column and full outer joins.

        Args:
            left_table (str): Name of the left table.
            right_table (str): Name of the right table.
            left_keys (list[str]): List of key columns in the left table.
            right_keys (list[str]): List of key columns in the right table.
            join_type (str): Type of join ('inner', 'left', 'right', 'full outer').

        Returns:
            dict: Resulting table as a dictionary of lists.
        """
        if left_table not in self.tables or right_table not in self.tables:
            raise ValueError("One or both tables do not exist.")

        if len(left_keys) != len(right_keys):
            raise ValueError("Number of join keys must match for both tables.")

        left = self.tables[left_table]
        right = self.tables[right_table]

        supported_join_types = ["inner", "left", "right", "full outer"]
        if join_type not in supported_join_types:
            raise ValueError(f"Join type must be one of: {supported_join_types}")
        else:
            result = left.join(
                right, keys=left_keys, right_keys=right_keys, join_type=join_type
            )

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
        sorted_table = table.sort_by(
            [(column_name, "ascending" if ascending else "descending")]
        )
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

        # Create the aggregation expression using string names
        aggregations = [(agg_column, agg_func)]

        grouped_table = table.group_by(group_column).aggregate(aggregations)
        return grouped_table.to_pydict()


if __name__ == "__main__":

    def benchmark(func, *args, **kwargs):
        """Simple benchmark wrapper that measures execution time"""
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration = (time.perf_counter() - start) * 1000  # Convert to milliseconds
        return result, duration

    engine = MiniQueryEngine()

    # Create sample tables
    engine.create_table(
        "left_table",
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        },
    )
    engine.create_table(
        "right_table",
        {
            "id": [2, 3, 4],
            "value": [200, 300, 400],
        },
    )

    # Perform a full outer join
    result, duration = benchmark(
        engine.join_tables,
        "left_table",
        "right_table",
        ["id"],
        ["id"],
        join_type="full outer",
    )
    print("Full Outer Join Result (took {:.2f}ms):".format(duration))
    print(result)

    # Filter the table
    filtered, duration = benchmark(engine.filter_table, "left_table", "value", ">", 15)
    print("\nFiltered Table (took {:.2f}ms):".format(duration))
    print(filtered)

    # Aggregate the table
    aggregated, duration = benchmark(
        engine.aggregate_table, "left_table", "value", "sum"
    )
    print("\nAggregated Table (took {:.2f}ms):".format(duration))
    print(aggregated)

    # Group by a column and apply an aggregation function
    grouped, duration = benchmark(engine.group_by, "left_table", "id", "value", "sum")
    print("\nGrouped Table (took {:.2f}ms):".format(duration))
    print(grouped)

    # Sort the table
    sorted_table, duration = benchmark(
        engine.sort_table, "left_table", "value", ascending=False
    )
    print("\nSorted Table (took {:.2f}ms):".format(duration))
    print(sorted_table)
