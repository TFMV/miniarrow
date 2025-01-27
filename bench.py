import time
import random
import string
from pyarrow import Table
from miniarrow import MiniQueryEngine


def generate_synthetic_data(rows, num_columns=5):
    """
    Generate synthetic data with random integers and strings.

    Args:
        rows (int): Number of rows to generate.
        num_columns (int): Number of columns (default is 5).

    Returns:
        dict: A dictionary representing synthetic data.
    """
    data = {}
    for col_id in range(num_columns):
        if col_id % 2 == 0:  # Even columns are integers
            data[f"int_col_{col_id}"] = [random.randint(0, 1000) for _ in range(rows)]
        else:  # Odd columns are strings
            data[f"str_col_{col_id}"] = [
                "".join(random.choices(string.ascii_letters, k=10)) for _ in range(rows)
            ]
    return data


def print_statistics(data):
    """
    Print statistics about the synthetic dataset.

    Args:
        data (dict): The synthetic data dictionary.
    """
    num_rows = len(next(iter(data.values())))
    num_columns = len(data)
    print("Dataset Statistics:")
    print(f"  Total Records: {num_rows}")
    print(f"  Total Columns: {num_columns}")


def benchmark_filter(engine, table_name, column_name, condition, value):
    """
    Benchmark the filtering operation.

    Args:
        engine (MiniQueryEngine): Instance of the query engine.
        table_name (str): Name of the table.
        column_name (str): Column to filter.
        condition (str): Filter condition.
        value: Value to compare.
    """
    start_time = time.perf_counter()
    engine.filter_table(table_name, column_name, condition, value)
    elapsed_time = time.perf_counter() - start_time
    print(f"Filter Benchmark: {elapsed_time:.6f} seconds")


def benchmark_aggregate(engine, table_name, column_name, agg_func):
    """
    Benchmark the aggregation operation.

    Args:
        engine (MiniQueryEngine): Instance of the query engine.
        table_name (str): Name of the table.
        column_name (str): Column to aggregate.
        agg_func (str): Aggregation function.
    """
    start_time = time.perf_counter()
    result = engine.aggregate_table(table_name, column_name, agg_func)
    elapsed_time = time.perf_counter() - start_time
    print(
        f"Aggregate Benchmark ({agg_func}): {elapsed_time:.6f} seconds | Result: {result}"
    )


def benchmark_sort(engine, table_name, column_name):
    """
    Benchmark the sorting operation.

    Args:
        engine (MiniQueryEngine): Instance of the query engine.
        table_name (str): Name of the table.
        column_name (str): Column to sort.
    """
    start_time = time.perf_counter()
    engine.sort_table(table_name, column_name)
    elapsed_time = time.perf_counter() - start_time
    print(f"Sort Benchmark: {elapsed_time:.6f} seconds")


def benchmark():
    """
    Perform benchmarks on the MiniQueryEngine.
    """
    # Create a synthetic dataset
    rows = 1_000_000
    num_columns = 5
    data = generate_synthetic_data(rows, num_columns)

    # Print dataset statistics
    print_statistics(data)

    # Initialize the query engine
    engine = MiniQueryEngine()
    engine.create_table("synthetic_table", data)

    # Benchmarks
    benchmark_filter(engine, "synthetic_table", "int_col_0", ">", 500)
    benchmark_aggregate(engine, "synthetic_table", "int_col_0", "sum")
    benchmark_sort(engine, "synthetic_table", "int_col_0")


if __name__ == "__main__":
    benchmark()
