# Getting Started: Hybrid Approach

**Best for**: Production applications that need both high-performance data ingestion and complex application logic.

!!! tip "Recommended for Production"
    Most real-world applications benefit from combining OGM's structure with bulk loading's performance.

## Why Combine Both?

- **OGM models** provide structure, validation, and type safety
- **Bulk loading** handles high-volume data efficiently
- **Use the right tool** for each task in your application

## Prerequisites

1. **Neo4j Database**: Running locally or remotely
   ```bash
   # Using Docker (recommended for testing)
   docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest
   ```

2. **Install Graphio**:
   ```bash
   pip install graphio
   ```

## Example: E-commerce Platform

Let's build a real-world example that uses both approaches effectively.

### Initial Setup

```python
from graphio import NodeModel, Relationship, Base
from neo4j import GraphDatabase
from datetime import datetime

# Set up connection for both approaches
driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
Base.set_driver(driver)
```

### Step 1: Define OGM Models for Structure

```python
class Product(NodeModel):
    _labels = ['Product']
    _merge_keys = ['sku']
    
    sku: str
    name: str
    price: float
    category: str
    
    # For application queries and bulk relationships
    purchased_by: Relationship = Relationship('Product', 'PURCHASED_BY', 'Customer')

class Customer(NodeModel):
    _labels = ['Customer']
    _merge_keys = ['email']
    
    email: str
    name: str
    membership_level: str = 'basic'

# Create indexes using OGM structure
Base.model_create_index()
```

### Step 2: Bulk Load Initial Data with Validation

```python
# Get bulk containers directly from OGM models
products = Product.dataset()    # Automatically uses Product's configuration
customers = Customer.dataset()  # Automatically uses Customer's configuration

# Import from your data sources
import pandas as pd

product_df = pd.read_csv('products.csv')
for _, row in product_df.iterrows():
    # Create validated OGM instance
    product = Product(
        sku=row['sku'], 
        name=row['name'],
        price=float(row['price']),
        category=row['category']
    )
    # Add validated instance directly to bulk dataset
    products.add(product)  # Pydantic validation + bulk performance

# Process customer data with validation
customer_df = pd.read_csv('customers.csv')
for _, row in customer_df.iterrows():
    try:
        customer = Customer(
            email=row['email'],
            name=row['name'],
            membership_level=row.get('membership_level', 'basic')
        )
        customers.add(customer)  # Validated data
    except ValueError as e:
        print(f"Invalid customer data: {e}")  # Catch validation errors

# Create relationship dataset from OGM model relationship
purchases = Product.purchased_by.dataset()  # Uses relationship configuration

# Process purchase data using OGM instances
purchase_df = pd.read_csv('purchases.csv')
for _, row in purchase_df.iterrows():
    # Create OGM instances for the relationship (these validate the lookup keys)
    product = Product(sku=row['product_sku'], name='', price=0.0, category='')  # Minimal for lookup
    customer = Customer(email=row['customer_email'], name='')  # Minimal for lookup
    
    # Add relationship using validated OGM instances
    purchases.add(
        product,    # OGM instance (automatic merge key extraction)
        customer,   # OGM instance (automatic merge key extraction)  
        {'purchase_date': row['date'], 'quantity': int(row['quantity'])}
    )

# Bulk load for performance (but with validation benefits!)
products.create(driver)
customers.create(driver) 
purchases.create(driver)   # Load relationships after nodes

print(f"✅ Loaded {len(products.nodes)} products, {len(customers.nodes)} customers")
print(f"✅ Created {len(purchases.relationships)} purchase relationships via bulk loading")
```

### Step 3: Use OGM for Application Logic

```python
# Application features use OGM for convenience
def get_product_recommendations(customer_email: str):
    # Find customer using OGM
    customer = Customer.match(Customer.email == customer_email).first()
    if not customer:
        return []
    
    # Query related products
    if customer.membership_level == 'premium':
        products = Product.match(Product.price < 100).all()
    else:
        products = Product.match(Product.category == 'electronics').all()
    
    return [p.name for p in products[:5]]

# Record purchases using OGM relationships
def record_purchase(customer_email: str, product_sku: str):
    customer = Customer.match(Customer.email == customer_email).first()
    product = Product.match(Product.sku == product_sku).first()
    
    if customer and product:
        product.purchased_by.add(customer, {'timestamp': datetime.now()})
        product.merge()  # Save the relationship
```

### Step 4: Ongoing Data Updates

```python
# For daily order imports, get RelationshipSet from OGM relationship
daily_orders = Product.purchased_by.dataset()  # Uses relationship configuration

# Process daily order file with OGM instances for relationships
orders_df = pd.read_csv('daily_orders.csv')
for _, order in orders_df.iterrows():
    # Find existing validated instances
    product = Product.match(Product.sku == order['product_sku']).first()
    customer = Customer.match(Customer.email == order['customer_email']).first()
    
    if product and customer:
        # Add relationship using validated OGM instances
        daily_orders.add(
            product,    # OGM instance (automatic merge key extraction)
            customer,   # OGM instance (automatic merge key extraction)
            {'timestamp': order['order_date'], 'quantity': order['quantity']}
        )

daily_orders.create(driver)
```

## Benefits of the Hybrid Approach

✅ **Data validation**: Pydantic validation catches errors before loading to Neo4j  
✅ **Performance**: Bulk loading for high-volume operations (10,000+ records efficiently)  
✅ **Developer experience**: Type hints, IDE support, intuitive queries  
✅ **Automatic configuration**: `Model.dataset()` uses model labels and merge keys  
✅ **Error handling**: Invalid data is caught early with clear error messages  
✅ **Consistency**: Same data structure for both application logic and bulk operations  

## Key Advantages of OGM + Bulk

### 🛡️ **Validation Benefits**
- **Type safety**: Pydantic ensures correct data types
- **Field validation**: Required fields, email format, value ranges
- **Early error detection**: Catch bad data before it reaches Neo4j
- **Consistent schema**: Same validation rules across all data entry points

### 🚀 **Performance Benefits** 
- **Bulk operations**: Handle thousands of records efficiently
- **Automatic batching**: Built-in chunking for large datasets
- **Index optimization**: Create indexes from OGM model structure
- **Memory efficient**: Process large files without loading everything into memory

### 🔧 **Developer Experience Benefits**
- **No configuration duplication**: `Person.dataset()` inherits model settings
- **IDE support**: Auto-completion and type checking
- **Merge key automation**: OGM instances automatically provide merge keys
- **Clean API**: `.add()` works with both dicts and instances  

## When to Use What

### Use OGM When:
- Building application features
- Need complex queries
- Working with individual records
- Need relationship traversal

### Use Bulk Loading When:
- Importing large datasets
- Daily batch updates
- ETL processes
- Initial data loading

## Next Steps

- **Deep dive into OGM** → [Complete OGM Guide](../details/ogm.md)
- **Master bulk loading** → [Bulk Loading Guide](../details/bulk.md)
- **Explore the API** → [API Reference](../api_reference/nodeset.md)