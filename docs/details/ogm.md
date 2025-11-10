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

GraphIO supports both **unidirectional** and **bidirectional** relationship definitions, making it easy to query relationships from both sides.

#### Basic Relationship Definition

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

#### Bidirectional Relationships (New Feature!)

**GraphIO automatically detects reverse relationships**, allowing you to define the same relationship from both node types for intuitive bidirectional querying:

```python
class Author(NodeModel):
    _labels = ['Author']
    _merge_keys = ['name']
    
    name: str
    birth_year: int
    
    # Forward relationship: Author -> Book
    books: Relationship = Relationship('Author', 'WROTE', 'Book')

class Book(NodeModel):
    _labels = ['Book']
    _merge_keys = ['isbn']
    
    title: str
    isbn: str
    published_year: int
    
    # Reverse relationship: same definition, but queryable from Book
    author: Relationship = Relationship('Author', 'WROTE', 'Book')

class Company(NodeModel):
    _labels = ['Company']
    _merge_keys = ['name']
    
    name: str
    industry: str
    
    # Forward relationship
    employees: Relationship = Relationship('Company', 'EMPLOYS', 'Person')

class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    
    name: str
    email: str
    
    # Reverse relationship - same relationship type, different query direction
    employer: Relationship = Relationship('Company', 'EMPLOYS', 'Person')
```

**Key Benefits:**
- ✅ **Same relationship definition** - no need for separate relationship types
- ✅ **Automatic direction detection** - GraphIO figures out the direction based on the querying node
- ✅ **Consistent database structure** - creates the same relationships regardless of which side initiates
- ✅ **Intuitive querying** - query from whichever side makes sense for your use case

**How it works:**
- Both definitions create the same relationship in Neo4j: `(Author)-[:WROTE]->(Book)`
- GraphIO automatically detects when you're querying from the "target" side and reverses the query direction
- Self-referencing relationships (e.g., `Person -> Person`) always work normally

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

#### Basic Relationship Queries

```python
# Find all companies Alice works at (forward direction)
alice_companies = alice.works_at.match().all()

# Filter relationship properties
from graphio import RelField
current_job = alice.works_at.filter(RelField('since') > '2023-01-01').match().all()

# Filter target node properties  
tech_companies = alice.works_at.match(Company.industry == 'Technology').all()
```

#### Bidirectional Relationship Queries (New!)

With bidirectional relationships, you can query from either side using natural syntax:

```python
# Create some data first
author = Author(name="Isaac Asimov", birth_year=1920)
book1 = Book(title="Foundation", isbn="978-0553293357", published_year=1951)
book2 = Book(title="I, Robot", isbn="978-0553382563", published_year=1950)

# Create relationships (can be done from either side!)
author.books.add(book1)
author.books.add(book2)
author.merge()

# Query 1: Forward direction (Author -> Books)
asimov = Author.match(Author.name == "Isaac Asimov").first()
asimov_books = asimov.books.match().all()
print([book.title for book in asimov_books])  # ['Foundation', 'I, Robot']

# Query 2: Reverse direction (Book -> Author) - Same relationship!
foundation = Book.match(Book.title == "Foundation").first()
book_author = foundation.author.match().first()
print(book_author.name)  # 'Isaac Asimov'

# Query 3: Bidirectional filtering
# Find books by authors born before 1925
old_author_books = Book.match().author.match(Author.birth_year < 1925).all()

# Find authors of books published after 1950
recent_book_authors = Author.match().books.match(Book.published_year > 1950).all()
```

#### Advanced Bidirectional Queries

```python
# Company-Employee example
class Company(NodeModel):
    _labels = ['Company']
    _merge_keys = ['name']
    name: str
    industry: str
    employees: Relationship = Relationship('Company', 'EMPLOYS', 'Person')

class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    name: str
    email: str
    age: int
    employer: Relationship = Relationship('Company', 'EMPLOYS', 'Person')

# Create test data
company = Company(name="TechCorp", industry="Technology")
alice = Person(name="Alice Smith", email="alice@example.com", age=30)
bob = Person(name="Bob Jones", email="bob@example.com", age=25)

# Add relationships with properties
company.employees.add(alice, {'position': 'Senior Developer', 'salary': 100000})
company.employees.add(bob, {'position': 'Junior Developer', 'salary': 70000})
company.merge()

# Forward queries: Company -> Employees
tech_corp = Company.match(Company.name == "TechCorp").first()
all_employees = tech_corp.employees.match().all()
senior_employees = tech_corp.employees.filter(RelField('position').contains('Senior')).match().all()

# Reverse queries: Person -> Company  
alice = Person.match(Person.email == "alice@example.com").first()
alice_employer = alice.employer.match().first()
print(f"{alice.name} works at {alice_employer.name}")

# Query employees by company industry (bidirectional)
tech_employees = Person.match().employer.match(Company.industry == "Technology").all()

# Query companies with young employees (bidirectional)
companies_with_young_staff = Company.match().employees.match(Person.age < 30).all()
```

#### Self-Referencing Relationships

Self-referencing relationships work exactly as before (unaffected by reverse relationship detection):

```python
class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    name: str
    email: str
    
    # Self-referencing relationships work normally
    friends: Relationship = Relationship('Person', 'FRIENDS_WITH', 'Person')
    mentors: Relationship = Relationship('Person', 'MENTORS', 'Person')

alice = Person(name="Alice", email="alice@example.com")
bob = Person(name="Bob", email="bob@example.com")
charlie = Person(name="Charlie", email="charlie@example.com")

alice.friends.add(bob)
bob.mentors.add(charlie)
alice.merge()
bob.merge()

# Self-referencing queries work as expected
alice_friends = alice.friends.match().all()
bob_mentors = bob.mentors.match().all()
```

#### Complex Traversals

For complex multi-hop traversals, use custom Cypher queries:

```python
from graphio.ogm.model import CypherQuery

# Find colleagues (people who work at the same company as Alice)
colleagues_query = CypherQuery("""
    MATCH (alice:Person {email: $email})-[:EMPLOYS]-(company:Company)-[:EMPLOYS]-(colleague:Person)
    WHERE colleague.email <> $email
    RETURN DISTINCT colleague
""", email="alice@example.com")

alice_colleagues = Person.match(colleagues_query).all()
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
    Person.age > 25
).all()

# Note: Complex relationship filtering requires custom Cypher
tech_workers_query = CypherQuery("""
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    WHERE p.age > 25 AND c.industry = 'Technology'
    RETURN DISTINCT p
""")
tech_workers = Person.match(tech_workers_query).all()
```

### Advanced Filtering

```python
# Comparison operators
young_adults = Person.match(Person.age >= 18, Person.age < 30).all()

# String operations
smiths = Person.match(Person.name.contains('Smith')).all()
alice_starts = Person.match(Person.name.starts_with('Ali')).all()
smith_ends = Person.match(Person.name.ends_with('Smith')).all()

# Complex filtering requires custom Cypher queries
case_insensitive_query = CypherQuery(
    "MATCH (n:Person) WHERE toLower(n.name) CONTAINS toLower($name) RETURN n", 
    name="alice"
)
alice_variations = Person.match(case_insensitive_query).all()

# List operations using custom Cypher
list_query = CypherQuery(
    "MATCH (n:Person) WHERE n.age IN $ages RETURN n",
    ages=[25, 30, 35]
)
specific_ages = Person.match(list_query).all()

# Null checks using custom Cypher
null_query = CypherQuery("MATCH (n:Person) WHERE n.age IS NULL RETURN n")
people_without_age = Person.match(null_query).all()
```

### Complex Queries

```python
# Complex queries require custom Cypher
complex_query = CypherQuery("""
    MATCH (p:Person)-[:WORKS_AT]->(c:Company), (p)-[:LIVES_IN]->(city:City)
    WHERE p.age > 25 AND c.industry = 'Technology' AND city.population > 1000000
    RETURN DISTINCT p
""")
tech_city_workers = Person.match(complex_query).all()

# Custom Cypher integration with regex
regex_query = CypherQuery("MATCH (n:Person) WHERE n.name =~ '.*Smith.*' RETURN n")
smiths = Person.match(regex_query).all()

# Count results with custom query
count_query = CypherQuery("MATCH (n:Person) WHERE n.age > 30 RETURN count(n) as person_count")
# Note: count() method not implemented - use custom query
with Base.get_driver().session() as session:
    result = session.run(count_query.query, count_query.params)
    person_count = result.single()['person_count']
```

---

## Model Registry and Index Management

### Global Registry

The OGM maintains a global registry of all model classes:

```python
from graphio import Base

# Registry automatically discovers models
print(Base.get_registry())  # Shows all registered NodeModel classes

# Create indexes for all models
Base.model_create_index()  # Creates indexes for all merge_keys

# Note: No built-in clear_graph() method - use custom Cypher if needed
# with Base.get_driver().session() as session:
#     session.run("MATCH (n) DETACH DELETE n")  # Be careful!
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
Base.model_create_index()  # Creates indexes for both Person and Company
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

## Multi-Database Support (Enterprise Edition)

!!! note "Enterprise Edition Only"
    Multi-database support is only available in Neo4j Enterprise Edition. Community Edition supports only a single database (typically `neo4j`).

### Configuration

Set a default database for all operations:

```python
from graphio import Base

# Set default database (optional)
Base.set_database('production')  # All operations use 'production' database
```

All OGM node operations accept an optional `database` parameter to override the default:

```python
# Use default database
person.create()
person.merge()
person.delete()

# Override for specific operation
person.create(database='staging')
person.merge(database='production')
```

Queries use the configured database:

```python
Base.set_database('analytics')
results = Person.match(Person.age > 25).all()  # Queries 'analytics' database
```

Bulk operations also support the `database` parameter:

```python
people = NodeSet(['Person'], merge_keys=['email'])
people.create(driver, database='production')
people.merge(driver, database='staging')
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
            # Create validated OGM instances
            person = Person(name=row['name'], email=row['email'], age=int(row['age']))  # Pydantic validation
            company = Company(name=row['company'], industry=row['industry'])  # Pydantic validation
            
            # Add validated instances directly to bulk containers
            people.add(person)    # No need for person.dict() - accepts OGM instances!
            companies.add(company) # Automatic property extraction
            
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
        products.add({
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
Base.model_create_index()  # Creates indexes for all registered models

# Create bulk containers that match your OGM structure
def create_matching_bulk_containers():
    containers = {}
    for model_name, model_class in Base.get_registry().items():
        containers[model_name] = model_class.dataset()  # Use dataset() method
    return containers

# Usage
bulk_containers = create_matching_bulk_containers()
# bulk_containers['User'].add({...})
# bulk_containers['Product'].add({...})

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
       bulk_container.add(person.model_dump())  # Load (Pydantic v2)
   ```

3. **Index Management**
   ```python
   # ✅ Good: Use OGM registry for consistent indexing
   Base.model_create_index()  # Creates indexes for all models
   
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
        Base.model_create_index()  # Set up all indexes
    
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
            products.add(product_data)
        
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
Base.model_create_index()

# Or for specific model
Person.create_index()
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
    people.add(person_data)
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
    Base.model_create_index()  # Create all indexes
    
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
   - **Use bidirectional relationships** for intuitive application development

2. **🚀 Performance**
   - Create indexes before querying (use `Base.model_create_index()`)
   - Use merge_keys for lookups when possible
   - Combine OGM with bulk loading for large datasets
   - **Bidirectional relationships have no performance impact** - same underlying database structure

3. **🔍 Queries**
   - Use specific filters on indexed properties
   - Leverage relationship traversal for complex queries
   - **Query from whichever side makes sense** for your use case with bidirectional relationships
   - Handle errors gracefully (connection, validation, query)

4. **📁 Organization**
   - Keep models in separate modules for large projects
   - Use the global registry for cross-model operations
   - Document your relationship patterns clearly
   - **Be consistent with bidirectional relationship naming** (e.g., `books`/`author`, `employees`/`employer`)

5. **🔗 Bidirectional Relationships (New!)**
   - **When to use**: Any relationship you might want to query from both sides
   - **Same relationship definition**: Use identical relationship parameters on both models  
   - **Automatic detection**: GraphIO automatically detects reverse relationships
   - **Creation flexibility**: Create relationships from either side - same result
   - **Self-referencing relationships**: Work exactly as before (no reverse detection)

For high-performance bulk data loading, see the [Bulk Loading Guide](bulk.md).