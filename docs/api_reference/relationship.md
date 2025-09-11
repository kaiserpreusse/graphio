# Relationship API Reference

The `Relationship` class is used to define relationships between NodeModel classes in the OGM.

## Features

- **Bidirectional Support**: Define the same relationship on both node types for intuitive querying
- **Automatic Direction Detection**: GraphIO automatically detects reverse relationships and generates appropriate Cypher queries
- **Relationship Properties**: Support for properties on relationships with filtering capabilities
- **Type Safety**: Full integration with Pydantic validation and Python type hints

## Bidirectional Relationships

GraphIO supports bidirectional relationship definitions, allowing you to query relationships from both directions using the same relationship definition:

```python
class Author(NodeModel):
    _labels = ['Author']
    _merge_keys = ['name']
    name: str
    
    # Forward relationship
    books: Relationship = Relationship('Author', 'WROTE', 'Book')

class Book(NodeModel):
    _labels = ['Book']
    _merge_keys = ['isbn']
    title: str
    isbn: str
    
    # Reverse relationship - same definition, automatically detected
    author: Relationship = Relationship('Author', 'WROTE', 'Book')

# Usage - query from either direction
author = Author.match(Author.name == "Isaac Asimov").first()
books = author.books.match().all()  # Forward

book = Book.match(Book.title == "Foundation").first()
author = book.author.match().first()  # Reverse - same relationship!
```

**Key Points:**
- Both relationships create the same structure in Neo4j: `(Author)-[:WROTE]->(Book)`
- GraphIO automatically detects when you're querying from the target side
- Self-referencing relationships (e.g., `Person -> Person`) work normally (no reverse detection)
- No performance impact - same underlying database relationships

## Class Definition

::: graphio.Relationship
    options:
      members:
        - __init__
        - match
        - filter
        - all
        - first
        - add
        - dataset
        - relationshipset
      show_source: false