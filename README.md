# Mini Arrow Query Engine

The Mini Arrow Query Engine is a lightweight, in-memory query engine built using Python and Apache Arrow. It provides functionality to filter, aggregate, and join data efficiently using the powerful Arrow library.

## Features

- **Create Tables**: Convert Python dictionaries into Apache Arrow tables.
- **Filtering**: Easily filter data based on conditions.
- **Aggregation**: Perform common aggregate functions like sum, average, count, etc.
- **Joining**: Join tables on specified columns. Join types supported: inner, left, right, full outer.

## Usage

Here's a simple example to get you started:

```python

# Initialize the query engine
engine = MiniQueryEngine()

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
```

```bash
☁  miniarrow [feature/bench] python bench.py
Dataset Statistics:
  Total Records: 1000000
  Total Columns: 5
Filter Benchmark: 0.602990 seconds
Aggregate Benchmark (sum): 0.001396 seconds | Result: 500536359
Sort Benchmark: 1.181422 seconds
```
