# NodeSet API Reference

The `NodeSet` class is used for bulk loading nodes into Neo4j. It provides methods for adding nodes, creating indexes, and loading data efficiently.

## Class Definition

::: graphio.NodeSet
    options:
      members:
        - __init__
        - add_node
        - add
        - add_nodes
        - create
        - merge
        - create_index
      show_source: false

## Deduplication

Starting with the latest version, NodeSet supports built-in deduplication to prevent duplicate nodes based on merge keys:

```python
# Enable deduplication
nodeset = NodeSet(['Person'], merge_keys=['email'], deduplicate=True)

# Add nodes - duplicates are automatically skipped
nodeset.add({'name': 'Alice', 'email': 'alice@example.com'})  
nodeset.add({'name': 'Alice Updated', 'email': 'alice@example.com'})  # Skipped

# Override deduplication for specific cases
nodeset.add({'name': 'Alice Forced', 'email': 'alice@example.com'}, force=True)  # Added
```

The deduplication feature:
- Uses an efficient internal index for O(1) duplicate detection
- Works with single or multiple merge keys
- Can be overridden on a per-node basis using `force=True`
- Applies to `add_node()`, `add()`, and `add_nodes()` methods