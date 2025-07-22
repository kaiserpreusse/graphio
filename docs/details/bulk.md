# Bulk Loading

Bulk loading with `NodeSet` and `RelationshipSet` is Graphio's high-performance approach for loading large datasets into Neo4j. This guide covers advanced patterns, optimization techniques, and best practices.

## Overview

Bulk loading is ideal for:

- **ETL processes** - Loading data from external sources
- **Data migration** - Moving data between Neo4j instances  
- **Initial data loading** - Populating a fresh database
- **Batch processing** - Regular data updates from feeds

Key advantages:

- ✅ **High performance** - Automatic batching (10,000 nodes/relationships per batch)
- ✅ **Memory efficient** - Processes data in chunks
- ✅ **Simple API** - Define containers, add data, load

---

## NodeSet: Bulk Node Loading

### Basic Usage

```python
from graphio import NodeSet
from neo4j import GraphDatabase

driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))

# Define container for Person nodes
people = NodeSet(['Person'], merge_keys=['email'])

# Add nodes
people.add_node({'name': 'Alice Smith', 'email': 'alice@example.com', 'age': 30})
people.add_node({'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 25})

# Bulk load to Neo4j
people.create(driver)
```

### Multiple Labels

Assign multiple labels to all nodes in a NodeSet:

```python
employees = NodeSet(['Person', 'Employee'], merge_keys=['employee_id'])
employees.add_node({'name': 'Alice', 'employee_id': 'E001', 'department': 'Engineering'})
```

### Merge Keys and Uniqueness

Merge keys define node uniqueness for MERGE operations:

```python
# Single merge key
users = NodeSet(['User'], merge_keys=['username'])

# Multiple merge keys (compound uniqueness)
locations = NodeSet(['Location'], merge_keys=['country', 'city'])
locations.add_node({'country': 'Germany', 'city': 'Munich', 'population': 1500000})
```

!!! tip "Performance Tip"
    Always create indexes for merge keys before bulk loading:
    ```python
    locations.create_index(driver)  # Creates indexes for country and city
    ```

### Default Properties

Apply properties to all nodes automatically:

```python
# All employees get a default department
employees = NodeSet(
    ['Person', 'Employee'], 
    merge_keys=['employee_id'],
    default_props={'company': 'ACME Corp', 'active': True}
)

employees.add_node({'name': 'Alice', 'employee_id': 'E001'})
# Result: Alice gets company='ACME Corp' and active=True automatically
```

### Handling Duplicates

**Option 1: Allow duplicates (fastest)**
```python
people.add_node({'name': 'Alice', 'email': 'alice@example.com'})
people.add_node({'name': 'Alice', 'email': 'alice@example.com'})  # Duplicate allowed
len(people.nodes)  # Returns 2
```

**Option 2: Prevent duplicates (slower)**
```python
people.add_unique({'name': 'Alice', 'email': 'alice@example.com'})
people.add_unique({'name': 'Alice', 'email': 'alice@example.com'})  # Ignored
len(people.nodes)  # Returns 1
```

!!! warning
    `add_unique()` checks all existing nodes, making it unsuitable for large datasets (>1000 nodes).

---

## RelationshipSet: Bulk Relationship Loading

### Basic Usage

```python
from graphio import RelationshipSet

# Define relationship container
employments = RelationshipSet(
    'WORKS_AT',           # Relationship type
    ['Person'],           # Start node labels
    ['Company'],          # End node labels
    ['email'],            # Start node matching properties
    ['name']              # End node matching properties
)

# Add relationships
employments.add_relationship(
    {'email': 'alice@example.com'},    # Start node matcher
    {'name': 'ACME Corp'},             # End node matcher  
    {'position': 'Developer', 'since': '2023-01-15'}  # Relationship properties
)

# Bulk load
employments.create(driver)
```

### Multiple Match Properties

Match nodes using multiple properties:

```python
# Match locations by country AND city
visits = RelationshipSet(
    'VISITED',
    ['Person'], ['Location'],
    ['email'], ['country', 'city']
)

visits.add_relationship(
    {'email': 'alice@example.com'},
    {'country': 'Germany', 'city': 'Munich'},
    {'date': '2023-06-15', 'duration_days': 5}
)
```

### Default Relationship Properties

```python
survey_responses = RelationshipSet(
    'RATED', ['Person'], ['Product'],
    ['user_id'], ['product_id'],
    default_props={'survey': 'Q3-2023', 'method': 'online'}
)

# Every relationship gets survey and method automatically
survey_responses.add_relationship(
    {'user_id': 'U001'}, 
    {'product_id': 'P456'}, 
    {'rating': 5}
)
```

---

## Performance Optimization

### Index Strategy

**Always create indexes before bulk loading:**

```python
# For NodeSets
people.create_index(driver)        # Creates index on email
companies.create_index(driver)     # Creates index on name

# For RelationshipSets  
employments.create_index(driver)   # Creates indexes on both Person(email) and Company(name)
```

**Compound indexes for multiple merge keys:**
```python
locations = NodeSet(['Location'], merge_keys=['country', 'city'])
locations.create_index(driver)  # Creates compound index on (country, city)
```

### Loading Order Matters

Load nodes before relationships:

```python
# ✅ Correct order
companies.create(driver)      # Load companies first
people.create(driver)         # Then people
employments.create(driver)    # Finally relationships

# ❌ Wrong order - relationships may fail if nodes don't exist
employments.create(driver)    # Relationships first - BAD!
people.create(driver)
companies.create(driver)
```

---

## Create vs Merge Operations

### Create Operation

- **Speed**: Fastest - no uniqueness checks
- **Use case**: Fresh databases, known unique data
- **Behavior**: Always creates new nodes/relationships

```python
# Creates all nodes, even duplicates
people.create(driver)
```

### Merge Operation

- **Speed**: Slower - checks for existing data
- **Use case**: Updating existing data, uncertain about duplicates
- **Behavior**: Updates existing, creates new

```python
# Updates existing nodes based on merge_keys, creates new ones
people.merge(driver)
```

### Advanced Merge Options

**Preserve specific properties during merge:**
```python
# Don't overwrite 'created_date' and 'original_source' on existing nodes
people.merge(driver, preserve=['created_date', 'original_source'])

# Equivalent Cypher behavior:
# ON CREATE SET n += properties
# ON MATCH SET n += properties EXCEPT created_date, original_source
```

**Append to array properties:**
```python
# Append to 'tags' array instead of replacing
articles = NodeSet(['Article'], merge_keys=['id'], append_props=['tags'])
articles.merge(driver)

# If node exists with tags=['tech'], and new node has tags=['python']
# Result: tags=['tech', 'python']
```

**Set merge behavior at creation:**
```python
# Define merge behavior when creating NodeSet
users = NodeSet(
    ['User'], ['username'], 
    preserve=['registration_date'],
    append_props=['login_history']
)
```

---

## Error Handling and Validation

### Connection Errors
```python
from neo4j.exceptions import ServiceUnavailable

try:
    people.create(driver)
except ServiceUnavailable:
    print("Neo4j database is not available")
```

### Data Validation
```python
def validate_email(email):
    return '@' in email

# Validate data before adding
for person_data in source_data:
    if validate_email(person_data.get('email', '')):
        people.add_node(person_data)
    else:
        print(f"Invalid email: {person_data}")
```

### Missing Reference Nodes
```python
# RelationshipSets don't validate that referenced nodes exist
# This relationship will be silently ignored if nodes don't exist
employments.add_relationship(
    {'email': 'nonexistent@example.com'},  # Node doesn't exist
    {'name': 'ACME Corp'},
    {'position': 'Developer'}
)
```

!!! warning "Important"
    Graphio does not validate that nodes referenced in relationships actually exist. Ensure proper loading order: nodes before relationships.

---

## Real-World Example: ETL Pipeline

```python
from graphio import NodeSet, RelationshipSet
from neo4j import GraphDatabase
import pandas as pd

def load_employee_data(csv_file, driver):
    # Read source data
    df = pd.read_csv(csv_file)
    
    # Create containers
    employees = NodeSet(['Person', 'Employee'], merge_keys=['employee_id'])
    departments = NodeSet(['Department'], merge_keys=['name'])
    works_in = RelationshipSet('WORKS_IN', ['Employee'], ['Department'], 
                              ['employee_id'], ['name'])
    
    # Process data
    for _, row in df.iterrows():
        # Add employee
        employees.add_node({
            'employee_id': row['id'],
            'name': row['full_name'],
            'email': row['email'],
            'hire_date': row['start_date']
        })
        
        # Add department
        departments.add_unique({  # Use unique to avoid duplicates
            'name': row['department'],
            'budget': row['dept_budget']
        })
        
        # Add relationship
        works_in.add_relationship(
            {'employee_id': row['id']},
            {'name': row['department']},
            {'start_date': row['start_date']}
        )
    
    # Create indexes for performance
    employees.create_index(driver)
    departments.create_index(driver)
    
    # Load data in correct order
    departments.merge(driver)  # Departments first
    employees.merge(driver)    # Then employees  
    works_in.merge(driver)     # Finally relationships
    
    print(f"Loaded {len(employees.nodes)} employees into {len(departments.nodes)} departments")

# Usage
driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
load_employee_data('employees.csv', driver)
```

This example demonstrates:

- Reading from external data source
- Processing data with validation
- Using appropriate merge strategies
- Creating indexes for performance
- Loading in correct order

---

## Best Practices Summary

1. **🚀 Performance**
   - Create indexes before bulk loading
   - Load nodes before relationships
   - Use `create()` for fresh data, `merge()` for updates

2. **💾 Memory Management**  
   - Process very large datasets in chunks
   - Use `add_unique()` sparingly (only for small datasets)

3. **🔍 Data Quality**
   - Validate data before adding to containers
   - Handle missing reference nodes appropriately
   - Use proper merge keys for your domain

4. **⚡ Optimization**
   - Batch related operations together
   - Use default properties to reduce repetitive data
   - Monitor loading progress for large datasets

## Integration with OGM

**Bulk loading works seamlessly with OGM models.** Use OGM to define your data structure and validation, then leverage bulk loading for high-performance operations.

### Why Combine OGM + Bulk Loading?

- **OGM provides**: Structure, validation, type safety, intuitive queries
- **Bulk loading provides**: High performance, memory efficiency, automatic batching
- **Together**: Best developer experience with maximum performance

### Quick Integration Example

```python
from graphio import NodeModel, NodeSet, Base

# Define structure with OGM
class Employee(NodeModel):
    _labels = ['Person', 'Employee']
    _merge_keys = ['employee_id']
    
    name: str
    employee_id: str
    email: str
    department: str

# Set up indexes using OGM
Base.set_driver(driver)
Base.create_indexes()

# Get bulk container directly from OGM model
employees = Employee.dataset()  # Automatically uses Employee's configuration

# Validate with OGM, load with bulk
import pandas as pd
df = pd.read_csv('employees.csv')

for _, row in df.iterrows():
    # Validate data structure with OGM
    emp_data = {
        'name': row['name'],
        'employee_id': row['id'], 
        'email': row['email'],
        'department': row['dept']
    }
    employee = Employee(**emp_data)  # Validates or raises error
    
    # Add to bulk container
    employees.add_node(employee.dict())

# Bulk load validated data
employees.create(driver)

# Query with OGM convenience
tech_employees = Employee.match(Employee.department == 'Technology').all()
```

### Integration Patterns

#### Pattern 1: Validation-First Loading

```python
def validated_bulk_load(model_class, data_source, driver):
    """Generic function to bulk load any OGM model with validation"""
    
    # Create bulk container using dataset() method
    bulk_container = model_class.dataset()
    
    valid_count = 0
    error_count = 0
    
    for record in data_source:
        try:
            # Validate with OGM model
            instance = model_class(**record)
            bulk_container.add_node(instance.dict())
            valid_count += 1
        except ValidationError as e:
            print(f"Skipping invalid record: {e}")
            error_count += 1
    
    # Bulk load validated data
    bulk_container.create(driver)
    
    return valid_count, error_count

# Usage with any model
class Product(NodeModel):
    _labels = ['Product']
    _merge_keys = ['sku']
    sku: str
    name: str
    price: float

loaded, errors = validated_bulk_load(Product, product_data, driver)
```

#### Pattern 2: Mixed Operations

```python
# Use different approaches for different operations
class InventoryManager:
    def __init__(self, driver):
        self.driver = driver
        Base.set_driver(driver)
    
    def daily_import(self, csv_file):
        """Bulk load daily inventory updates"""
        products = Product.dataset()  # Use Product model's dataset
        
        df = pd.read_csv(csv_file)
        for _, row in df.iterrows():
            products.add_node({
                'sku': row['sku'],
                'stock_count': row['stock'],
                'last_updated': datetime.now()
            })
        
        products.merge(self.driver)  # Update existing products
        return len(products.nodes)
    
    def price_adjustment(self, sku: str, new_price: float):
        """Individual price update using OGM"""
        product = Product.match(Product.sku == sku).first()
        if product:
            product.price = new_price
            product.merge()
            return True
        return False
```

### Performance Benefits

When you combine OGM + bulk loading effectively:

```python
# ❌ Slow: Individual OGM operations for bulk data
for record in large_dataset:  # 10,000 records
    person = Person(**record)
    person.merge()  # 10,000 individual database calls!

# ✅ Fast: Validate with OGM, load with bulk
people = Person.dataset()  # Use dataset() method
for record in large_dataset:  # 10,000 records
    person = Person(**record)  # Validate locally
    people.add_node(person.dict())  # Add to batch

people.create(driver)  # Single efficient bulk operation
```

### Real-World Integration: Data Pipeline

```python
class ETLPipeline:
    def __init__(self, driver):
        self.driver = driver
        Base.set_driver(driver)
        Base.create_indexes()  # Create all OGM model indexes
    
    def process_customers(self, customer_file):
        """Load customer data with validation"""
        customers = Customer.dataset()  # Use Customer model's dataset
        addresses = Address.dataset()   # Use Address model's dataset
        lives_at = Customer.lives_at.dataset()  # Use relationship's dataset
        
        df = pd.read_csv(customer_file)
        for _, row in df.iterrows():
            # Validate customer data
            customer_data = {
                'email': row['email'],
                'name': row['name'],
                'phone': row['phone']
            }
            Customer(**customer_data)  # Validates
            
            # Validate address data  
            address_data = {
                'address_id': row['address_id'],
                'street': row['street'],
                'city': row['city'],
                'country': row['country']
            }
            Address(**address_data)  # Validates
            
            # Add to bulk containers
            customers.add_node(customer_data)
            addresses.add_node(address_data)
            lives_at.add_relationship(
                {'email': row['email']},
                {'address_id': row['address_id']},
                {'since': row['move_in_date']}
            )
        
        # Load in correct order
        addresses.create(self.driver)
        customers.create(self.driver) 
        lives_at.create(self.driver)
        
        return len(customers.nodes)
    
    def customer_analytics(self):
        """Use OGM for complex analytics after bulk loading"""
        # Find customers in major cities
        major_city_customers = (Customer.match()
                               .where(Customer.lives_at.target().city.in_(['London', 'Paris', 'Berlin']))
                               .all())
        
        return [c.name for c in major_city_customers]

# Usage
pipeline = ETLPipeline(driver)
loaded = pipeline.process_customers('customers.csv')  # Bulk + validation
analytics = pipeline.customer_analytics()  # OGM queries
```

### Integration Best Practices

1. **Use OGM for structure definition**
   ```python
   # Define once with OGM
   class Product(NodeModel):
       _labels = ['Product'] 
       _merge_keys = ['sku']
   
   # Get bulk container directly from model
   products = Product.dataset()  # Automatically matches model configuration
   ```

2. **Leverage OGM validation**
   ```python
   # Validate before adding to bulk container
   for data in source:
       model_instance = ProductModel(**data)  # Validates
       bulk_container.add_node(model_instance.dict())
   ```

3. **Use registry for index management**
   ```python
   # Create indexes for all models at once
   Base.create_indexes()
   
   # Then use bulk loading with existing indexes
   products.create(driver)
   ```

4. **Choose the right approach for each task**
   ```python
   # Bulk loading: High-volume, repetitive operations
   products.create(driver)  # Load 10,000 products
   
   # OGM: Application logic, complex queries
   product = Product.match(Product.sku == 'ABC123').first()
   recommendations = product.similar_products.match().all()
   ```

This hybrid approach gives you the best of both worlds: the structure and developer experience of OGM with the performance of bulk loading.

For complete OGM documentation, see the [OGM Guide](ogm.md).
