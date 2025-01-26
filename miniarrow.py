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
        if condition == "==":
            mask = pc.equal(column, value)
        elif condition == ">":
            mask = pc.greater(column, value)
        elif condition == "<":
            mask = pc.less(column, value)
        elif condition == ">=":
            mask = pc.greater_equal(column, value)
        elif condition == "<=":
            mask = pc.less_equal(column, value)
        elif condition == "!=":
            mask = pc.not_equal(column, value)
        else:
            raise ValueError("Unsupported condition. Use one of: ==, >, <, >=, <=, !=")

        # Filter the table
        filtered_table = table.filter(mask)
        return filtered_table

    def aggregate_table(self, table_name, column_name, agg_func):
        """
        Aggregate a column in a table.
        Supported functions: 'sum', 'mean', 'min', 'max', 'count'.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.tables[table_name]
        column = table[column_name]

        if agg_func == "sum":
            return pc.sum(column).as_py()
        elif agg_func == "mean":
            return pc.mean(column).as_py()
        elif agg_func == "min":
            return pc.min(column).as_py()
        elif agg_func == "max":
            return pc.max(column).as_py()
        elif agg_func == "count":
            return len(column)
        else:
            raise ValueError("Unsupported aggregation function. Use one of: sum, mean, min, max, count")

    def join_tables(self, left_table, right_table, left_key, right_key):
        """
        Perform an inner join between two tables.
        """
        if left_table not in self.tables or right_table not in self.tables:
            raise ValueError("One or both tables do not exist.")

        left = self.tables[left_table]
        right = self.tables[right_table]

        return left.join(right, keys=[left_key, right_key])


# Example Usage
if __name__ == "__main__":
    engine = MiniQueryEngine()

    # Create sample tables
    engine.create_table("users", {
        "user_id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [25, 30, 35, 40]
    })

    engine.create_table("orders", {
        "order_id": [101, 102, 103],
        "user_id": [1, 2, 3],
        "amount": [250, 450, 300]
    })

    # Filter users older than 30
    filtered_users = engine.filter_table("users", "age", ">", 30)
    print("Filtered Users:")
    print(filtered_users)

    # Aggregate total order amount
    total_amount = engine.aggregate_table("orders", "amount", "sum")
    print("\nTotal Order Amount:", total_amount)

    # Join users and orders
    joined_table = engine.join_tables("users", "orders", "user_id", "user_id")
    print("\nJoined Table:")
    print(joined_table)
