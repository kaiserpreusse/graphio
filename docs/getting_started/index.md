# Getting Started

Welcome to Graphio! Choose the approach that fits your use case:

=== "OGM"
    For applications with complex data models and queries. [Learn more →](ogm.md)

=== "Bulk Loading"
    For high-performance data ingestion and ETL processes. [Learn more →](bulk.md)

=== "Hybrid (Recommended)"
    Combine both approaches for production applications. [Learn more →](hybrid.md)

## Quick Comparison

| Feature | OGM | Bulk Loading | Hybrid |
|---------|-----|--------------|--------|
| **Performance** | Good for individual operations | Excellent for large datasets | Excellent overall |
| **Data Validation** | ✅ Built-in with Pydantic | ❌ Manual | ✅ Best of both |
| **Type Safety** | ✅ Full type hints | ❌ Dictionary-based | ✅ Where needed |
| **Learning Curve** | Moderate | Easy | Moderate |
| **Use Case** | Applications | ETL/Migration | Production apps |

## Prerequisites

Regardless of which approach you choose, you'll need:

1. **Neo4j Database** running locally or remotely
   ```bash
   # Quick start with Docker
   docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest
   ```

2. **Graphio installed**
   ```bash
   pip install graphio
   ```

## Not Sure Which to Choose?

- **Building an application?** → Start with [OGM Track](ogm.md)
- **Loading lots of data?** → Start with [Bulk Loading Track](bulk.md)
- **Building for production?** → Start with [Hybrid Approach](hybrid.md)

!!! tip "Pro Tip"
    You're not locked into one approach! Graphio is designed so you can use OGM and bulk loading together in the same project. Many users start with one approach and add the other as their needs grow.