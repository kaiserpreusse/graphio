# Object Graph Mapper (OGM)

Graphio's Object Graph Mapper provides an intuitive, Pydantic-based approach for modeling complex graph data. Build applications with type safety, relationship traversal, and powerful querying capabilities.

## Overview

The OGM is ideal for:

- **Applications** - Web apps, APIs, data analysis tools
- **Complex data models** - Multiple relationships, inheritance, validation
- **Interactive queries** - Dynamic filtering, relationship traversal
- **Type safety** - Leverage Python's type system with Pydantic validation

Key advantages:

- ✅ **Pydantic integration** - Automatic validation, serialization, IDE support
- ✅ **Intuitive syntax** - `Person.match(Person.age > 25).all()`
- ✅ **Relationship traversal** - `alice.works_at.match().all()`
- ✅ **Global registry** - Automatic model discovery and index management

---

## Basic Model Definition

### Simple Node Model

```python
from graphio import NodeModel, Base
from neo4j import GraphDatabase

# Set up connection (required for all OGM operations)
driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
Base.set_driver(driver)

class Person(NodeModel):
    # Graphio configuration
    _labels = ['Person']
    _merge_keys = ['email']
    
    # Pydantic fields with full validation
    name: str
    email: str
    age: int
    active: bool = True  # Default value
```

### Working with Instances

```python
# Create instance
alice = Person(name='Alice Smith', email='alice@example.com', age=30)

# Validate data (automatic with Pydantic)
# alice = Person(name='Alice', email='invalid-email', age='thirty')  # Raises ValidationError

# Save to Neo4j
alice.merge()  # Creates or updates based on merge_keys

# Create without checking for existing
alice.create()  # Always creates new node
```

---

## Relationships

### Defining Relationships

```python
from graphio import Relationship

class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    
    name: str
    email: str
    
    # Define relationships
    works_at: Relationship = Relationship('Person', 'WORKS_AT', 'Company')
    friends_with: Relationship = Relationship('Person', 'FRIENDS_WITH', 'Person')
    lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

class Company(NodeModel):
    _labels = ['Company']  
    _merge_keys = ['name']
    
    name: str
    industry: str

class City(NodeModel):
    _labels = ['City']
    _merge_keys = ['name', 'country']
    
    name: str
    country: str
    population: int
```

### Working with Relationships

```python
# Create nodes
alice = Person(name='Alice Smith', email='alice@example.com')
acme = Company(name='ACME Corp', industry='Technology')
munich = City(name='Munich', country='Germany', population=1500000)

# Add relationships with properties
alice.works_at.add(acme, {'position': 'Developer', 'since': '2023-01-15'})
alice.lives_in.add(munich, {'since': '2020-06-01'})

# Save everything
alice.merge()  # Saves alice and all its relationships
```

### Relationship Queries

```python
# Find all companies Alice works at
alice_companies = alice.works_at.match().all()

# Filter relationship properties
current_job = alice.works_at.match().where('r.since > "2023-01-01"').all()

# Filter target node properties  
tech_companies = alice.works_at.match(Company.industry == 'Technology').all()

# Chain relationship traversals
alice_colleagues = alice.works_at.target().match().source().friends_with.match().all()
```

---

## Querying

### Basic Queries

```python
# Find all persons
all_people = Person.match().all()

# Find specific person
alice = Person.match(Person.email == 'alice@example.com').first()

# Multiple conditions
tech_workers = Person.match(
    Person.age > 25,
    Person.works_at.target().industry == 'Technology'
).all()
```

### Advanced Filtering

```python
# Comparison operators
young_adults = Person.match(Person.age >= 18, Person.age < 30).all()

# String operations
smiths = Person.match(Person.name.contains('Smith')).all()
alice_variations = Person.match(Person.name.ilike('ali%')).all()

# List operations
target_ages = [25, 30, 35]
specific_ages = Person.match(Person.age.in_(target_ages)).all()

# Null checks
people_without_age = Person.match(Person.age.is_null()).all()
```

### Complex Queries

```python
# Combine multiple models
query = (Person.match(Person.age > 25)
         .where(Person.works_at.target().industry == 'Technology')
         .where(Person.lives_in.target().population > 1000000))

tech_city_workers = query.all()

# Custom Cypher integration
custom_query = Person.match().where("n.name =~ '.*Smith.*'").all()

# Count results
person_count = Person.match(Person.age > 30).count()
```

---

## Model Registry and Index Management

### Global Registry

The OGM maintains a global registry of all model classes:

```python
from graphio import Base

# Registry automatically discovers models
print(Base.registry)  # Shows all registered NodeModel classes

# Create indexes for all models
Base.create_indexes()  # Creates indexes for all merge_keys

# Clear all data (be careful!)
Base.clear_graph()
```

### Auto-Discovery

Models are automatically registered when imported:

```python
# models/person.py
class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    name: str
    email: str

# models/company.py  
class Company(NodeModel):
    _labels = ['Company']
    _merge_keys = ['name']
    name: str
    industry: str

# main.py
from models.person import Person
from models.company import Company

# Both models are now in the registry automatically
Base.create_indexes()  # Creates indexes for both Person and Company
```

---

## Advanced Features

### Multiple Labels

```python
class Employee(NodeModel):
    _labels = ['Person', 'Employee']  # Multiple labels
    _merge_keys = ['employee_id']
    
    name: str
    employee_id: str
    department: str
```

### Compound Merge Keys

```python
class Location(NodeModel):
    _labels = ['Location']
    _merge_keys = ['country', 'city']  # Compound uniqueness
    
    country: str
    city: str
    population: int
    
# Creates compound index on (country, city)
```

### Default Properties

```python
from datetime import datetime

class User(NodeModel):
    _labels = ['User']
    _merge_keys = ['username']
    
    username: str
    email: str
    created_at: datetime = datetime.now()  # Default value
    active: bool = True
```

### Model Inheritance

```python
class BasePerson(NodeModel):
    _labels = ['Person']
    
    name: str
    email: str

class Employee(BasePerson):
    _labels = ['Person', 'Employee']  # Override labels
    _merge_keys = ['employee_id']
    
    employee_id: str
    department: str
    
    # Inherits name and email from BasePerson
```

### Custom Validation

```python
from pydantic import validator

class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    
    name: str
    email: str
    age: int
    
    @validator('email')
    def email_must_be_valid(cls, v):
        if '@' not in v:
            raise ValueError('Invalid email address')
        return v
    
    @validator('age')
    def age_must_be_positive(cls, v):
        if v < 0:
            raise ValueError('Age must be positive')
        return v
```

---

## Integration with Bulk Loading

**The OGM and bulk loading are designed to work together seamlessly.** Use OGM models to define structure and validation, then leverage bulk loading for high-performance data operations when needed.

### Common Integration Patterns

#### Pattern 1: Model-Driven Bulk Loading

Use OGM models to ensure data consistency in bulk operations:

```python
from graphio import NodeSet, RelationshipSet

# Define OGM models for structure and validation
class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    name: str
    email: str
    age: int

class Company(NodeModel):
    _labels = ['Company']
    _merge_keys = ['name']
    name: str
    industry: str

# Create bulk containers using model dataset() method
def bulk_load_with_validation(csv_file):
    people = Person.dataset()    # Automatically uses Person's configuration
    companies = Company.dataset()  # Automatically uses Company's configuration
    
    # Process CSV data with validation
    import pandas as pd
    df = pd.read_csv(csv_file)
    
    for _, row in df.iterrows():
        try:
            # Validate each record with OGM model
            person_data = {'name': row['name'], 'email': row['email'], 'age': int(row['age'])}
            person = Person(**person_data)  # Validates structure and types
            
            company_data = {'name': row['company'], 'industry': row['industry']}
            company = Company(**company_data)  # Validates structure
            
            # Add to bulk containers after validation
            people.add_node(person.dict())
            companies.add_node(company.dict())
            
        except ValidationError as e:
            print(f"Skipping invalid record: {e}")
    
    # Bulk load validated data
    companies.create(driver)  # Load companies first
    people.create(driver)     # Then people
    
    return len(people.nodes), len(companies.nodes)

# Usage
loaded_people, loaded_companies = bulk_load_with_validation('employees.csv')
```

#### Pattern 2: Bulk Load + OGM Queries

Load data in bulk, then use OGM for application queries:

```python
# Step 1: Bulk load initial dataset  
def initial_data_load():
    # Load thousands of records efficiently
    users = User.dataset()     # Uses User model configuration
    products = Product.dataset()  # Uses Product model configuration
    
    # ... bulk loading logic ...
    users.create(driver)
    products.create(driver)

# Step 2: Use OGM for application features
class User(NodeModel):
    _labels = ['User']
    _merge_keys = ['user_id']
    user_id: str
    name: str
    email: str
    
    purchased: Relationship = Relationship('User', 'PURCHASED', 'Product')

def get_user_recommendations(user_id: str):
    # OGM makes complex queries simple
    user = User.match(User.user_id == user_id).first()
    if not user:
        return []
    
    # Find products purchased by similar users
    similar_purchases = (user.purchased.target()
                        .match()  # Get purchased products
                        .source()  # Get users who purchased them
                        .purchased.match()  # Get their other purchases
                        .all())
    
    return [p.name for p in similar_purchases[:10]]
```

#### Pattern 3: Hybrid Updates

Use different approaches for different types of updates:

```python
class Product(NodeModel):
    _labels = ['Product']
    _merge_keys = ['sku']
    sku: str
    name: str
    price: float
    stock_count: int

# Individual price updates (OGM - good for single items)
def update_product_price(sku: str, new_price: float):
    product = Product.match(Product.sku == sku).first()
    if product:
        product.price = new_price
        product.merge()
        return True
    return False

# Bulk inventory updates (Bulk loading - good for many items)
def daily_inventory_update(inventory_file: str):
    products = Product.dataset()  # Uses Product model configuration
    
    # Process inventory file
    import pandas as pd
    df = pd.read_csv(inventory_file)
    
    for _, row in df.iterrows():
        products.add_node({
            'sku': row['sku'],
            'stock_count': int(row['new_stock']),
            # Only update stock, preserve other properties
        })
    
    # Use merge to update existing products
    products.merge(driver, preserve=['name', 'price'])  # Don't overwrite these fields
```

#### Pattern 4: Model Registry Integration

Leverage the global registry for bulk operations:

```python
from graphio import Base

# Define all your models
class User(NodeModel):
    _labels = ['User']
    _merge_keys = ['email']
    email: str
    name: str

class Product(NodeModel):
    _labels = ['Product']
    _merge_keys = ['sku']
    sku: str
    name: str

class Order(NodeModel):
    _labels = ['Order']
    _merge_keys = ['order_id']
    order_id: str
    total: float

# Use registry to create all indexes at once
Base.create_indexes()  # Creates indexes for all registered models

# Create bulk containers that match your OGM structure
def create_matching_bulk_containers():
    containers = {}
    for model_name, model_class in Base.registry.items():
        containers[model_name] = model_class.dataset()  # Use dataset() method
    return containers

# Usage
bulk_containers = create_matching_bulk_containers()
# bulk_containers['User'].add_node({...})
# bulk_containers['Product'].add_node({...})

# Or even simpler, just use model.dataset() directly:
users = User.dataset()
products = Product.dataset()
orders = Order.dataset()
```

### Best Practices for Integration

1. **Design Consistency**
   ```python
   # ✅ Best: Use dataset() method for automatic consistency
   class Person(NodeModel):
       _labels = ['Person']
       _merge_keys = ['email']
   
   people_bulk = Person.dataset()  # Automatically matches model configuration
   ```

2. **Validation Strategy**
   ```python
   # ✅ Good: Validate with OGM before bulk loading
   for data in large_dataset:
       person = Person(**data)  # Validate
       bulk_container.add_node(person.dict())  # Load
   ```

3. **Index Management**
   ```python
   # ✅ Good: Use OGM registry for consistent indexing
   Base.create_indexes()  # Creates indexes for all models
   
   # Then use bulk loading knowing indexes exist
   people.create(driver)  # Will use existing indexes
   ```

4. **Performance Optimization**
   ```python
   # ✅ Good: Choose the right tool for the task
   
   # Bulk loading for high-volume operations
   def import_customer_data():
       customers = Customer.dataset()  # Use dataset() method
       # ... bulk load thousands of records
   
   # OGM for application logic
   def process_customer_order(email: str):
       customer = Customer.match(Customer.email == email).first()
       # ... complex business logic
   ```

### Real-World Integration Example

```python
# Complete e-commerce system using both approaches
class EcommerceSystem:
    def __init__(self, driver):
        self.driver = driver
        Base.set_driver(driver)
        Base.create_indexes()  # Set up all indexes
    
    def import_catalog(self, product_csv: str):
        """Use bulk loading for importing product catalog"""
        products = Product.dataset()  # Use dataset() method
        
        df = pd.read_csv(product_csv)
        for _, row in df.iterrows():
            # Validate with OGM model
            product_data = {
                'sku': row['sku'],
                'name': row['name'],
                'price': float(row['price']),
                'category': row['category']
            }
            Product(**product_data)  # Validates
            products.add_node(product_data)
        
        products.create(self.driver)
        return len(products.nodes)
    
    def place_order(self, customer_email: str, product_skus: list):
        """Use OGM for complex order processing"""
        customer = Customer.match(Customer.email == customer_email).first()
        if not customer:
            raise ValueError("Customer not found")
        
        order_total = 0
        for sku in product_skus:
            product = Product.match(Product.sku == sku).first()
            if product:
                customer.purchased.add(product, {'timestamp': datetime.now()})
                order_total += product.price
        
        customer.merge()  # Save relationships
        return order_total
    
    def daily_analytics(self):
        """Use OGM for complex analytics queries"""
        # Find top customers by purchase volume
        top_customers = (Customer.match()
                        .where("SIZE((c)-[:PURCHASED]->()) > 5")
                        .all())
        
        return [c.email for c in top_customers]

# Usage
system = EcommerceSystem(driver)
system.import_catalog('products.csv')  # Bulk loading
total = system.place_order('alice@example.com', ['SKU001', 'SKU002'])  # OGM
top_buyers = system.daily_analytics()  # OGM queries
```

This integration approach gives you:

- **Structure and validation** from OGM models
- **Performance** from bulk loading operations  
- **Developer experience** from type hints and intelligent queries
- **Flexibility** to use the right tool for each task

---

## Error Handling

### Connection Errors

```python
from neo4j.exceptions import ServiceUnavailable

try:
    alice = Person.match(Person.email == 'alice@example.com').first()
except ServiceUnavailable:
    print("Database connection failed")
```

### Validation Errors

```python
from pydantic import ValidationError

try:
    invalid_person = Person(name='Alice', email='invalid', age='thirty')
except ValidationError as e:
    print(f"Validation failed: {e}")
    # Handle validation errors appropriately
```

### Query Errors

```python
from neo4j.exceptions import CypherSyntaxError

try:
    # This might fail if relationship doesn't exist
    results = Person.match().where("INVALID CYPHER").all()
except CypherSyntaxError as e:
    print(f"Query failed: {e}")
```

---

## Performance Best Practices

### Index Creation

```python
# Create indexes before loading data
Base.create_indexes()

# Or for specific model
Person.create_indexes()
```

### Efficient Queries

```python
# ✅ Good: Use merge_keys for lookups
alice = Person.match(Person.email == 'alice@example.com').first()

# ❌ Avoid: Filtering on non-indexed properties
slow_query = Person.match(Person.name.contains('Alice')).all()

# ✅ Better: Create index on name if needed
# Then use indexed lookups when possible
```

### Batch Operations

```python
# For bulk operations, use NodeSet/RelationshipSet
# For individual operations, use OGM instances

# ✅ Bulk loading
people = NodeSet(['Person'], merge_keys=['email'])
for person_data in large_dataset:
    people.add_node(person_data)
people.create(driver)

# ✅ Individual operations
alice = Person(name='Alice', email='alice@example.com')
alice.merge()
```

---

## Real-World Example: Social Network

```python
from graphio import NodeModel, Relationship, Base
from neo4j import GraphDatabase
from datetime import datetime
from typing import Optional

# Set up connection
driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
Base.set_driver(driver)

class User(NodeModel):
    _labels = ['User']
    _merge_keys = ['username']
    
    username: str
    email: str
    full_name: str
    created_at: datetime = datetime.now()
    active: bool = True
    
    # Relationships
    follows: Relationship = Relationship('User', 'FOLLOWS', 'User')
    posted: Relationship = Relationship('User', 'POSTED', 'Post')
    likes: Relationship = Relationship('User', 'LIKES', 'Post')

class Post(NodeModel):
    _labels = ['Post']
    _merge_keys = ['post_id']
    
    post_id: str
    content: str
    created_at: datetime = datetime.now()
    likes_count: int = 0

def create_social_network():
    # Create users
    alice = User(username='alice_smith', email='alice@example.com', full_name='Alice Smith')
    bob = User(username='bob_jones', email='bob@example.com', full_name='Bob Jones')
    charlie = User(username='charlie_brown', email='charlie@example.com', full_name='Charlie Brown')
    
    # Create posts
    post1 = Post(post_id='post_001', content='Hello Graphio!')
    post2 = Post(post_id='post_002', content='Learning Neo4j with Python')
    
    # Build relationships
    alice.follows.add(bob, {'since': datetime.now()})
    alice.follows.add(charlie, {'since': datetime.now()})
    bob.follows.add(alice, {'since': datetime.now()})
    
    alice.posted.add(post1, {'timestamp': datetime.now()})
    bob.posted.add(post2, {'timestamp': datetime.now()})
    
    alice.likes.add(post2, {'timestamp': datetime.now()})
    bob.likes.add(post1, {'timestamp': datetime.now()})
    
    # Save to database
    Base.create_indexes()  # Create all indexes
    
    alice.merge()  # Saves alice and all connected data
    bob.merge()    # Saves bob and all connected data
    charlie.merge() # Saves charlie

def query_social_network():
    # Find users Alice follows
    alice = User.match(User.username == 'alice_smith').first()
    following = alice.follows.match().all()
    print(f"Alice follows: {[user.full_name for user in following]}")
    
    # Find posts liked by Bob
    bob = User.match(User.username == 'bob_jones').first()
    liked_posts = bob.likes.match().all()
    print(f"Bob liked: {[post.content for post in liked_posts]}")
    
    # Find active users who posted recently
    from datetime import timedelta
    recent_posters = (User.match(User.active == True)
                     .where(User.posted.target().created_at > datetime.now() - timedelta(days=7))
                     .all())
    
    print(f"Recent active posters: {[user.full_name for user in recent_posters]}")

# Usage
create_social_network()
query_social_network()
```

This example demonstrates:

- Complex model definitions with relationships
- Default values and validation
- Relationship creation with properties
- Advanced querying patterns
- Real-world application structure

---

## Best Practices Summary

1. **🏗️ Model Design**
   - Use meaningful merge_keys for your domain
   - Leverage Pydantic validation for data quality
   - Define relationships clearly with proper labels

2. **🚀 Performance**
   - Create indexes before querying (use `Base.create_indexes()`)
   - Use merge_keys for lookups when possible
   - Combine OGM with bulk loading for large datasets

3. **🔍 Queries**
   - Use specific filters on indexed properties
   - Leverage relationship traversal for complex queries
   - Handle errors gracefully (connection, validation, query)

4. **📁 Organization**
   - Keep models in separate modules for large projects
   - Use the global registry for cross-model operations
   - Document your relationship patterns clearly

For high-performance bulk data loading, see the [Bulk Loading Guide](bulk.md).