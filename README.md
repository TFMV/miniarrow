# Mini Arrow Query Engine

The Mini Arrow Query Engine is a lightweight, in-memory query engine built using Python and Apache Arrow. It provides functionality to filter, aggregate, and join data efficiently using the powerful Arrow library.

## Features

- **Create Tables**: Convert Python dictionaries into Apache Arrow tables.
- **Filtering**: Easily filter data based on conditions.
- **Aggregation**: Perform common aggregate functions like sum, average, count, etc.
- **Joining**: Join tables on specified columns.

## Installation

```bash
pip install miniarrow
```

## Usage

Here's a simple example to get you started:

```python
from mini_arrow_engine import MiniQueryEngine

# Initialize the query engine
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
```
