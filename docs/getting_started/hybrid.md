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
Base.create_indexes()
```

### Step 2: Bulk Load Initial Data

```python
# Get bulk containers directly from OGM models
products = Product.dataset()    # Automatically uses Product's configuration
customers = Customer.dataset()  # Automatically uses Customer's configuration

# Import from your data sources
import pandas as pd

product_df = pd.read_csv('products.csv')
for _, row in product_df.iterrows():
    # Validate with OGM before bulk loading
    product_data = {
        'sku': row['sku'], 
        'name': row['name'],
        'price': float(row['price']),
        'category': row['category']
    }
    Product(**product_data)  # Validates data structure
    products.add_node(product_data)

# Bulk load for performance
products.create(driver)
customers.create(driver)

print(f"Loaded {len(products.nodes)} products via bulk loading")
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

# Process daily order file
orders_df = pd.read_csv('daily_orders.csv')
for _, order in orders_df.iterrows():
    daily_orders.add_relationship(
        {'sku': order['product_sku']},
        {'email': order['customer_email']},
        {'timestamp': order['order_date'], 'quantity': order['quantity']}
    )

daily_orders.create(driver)
```

## Benefits of the Hybrid Approach

✅ **Data validation**: OGM models ensure data quality  
✅ **Performance**: Bulk loading for high-volume operations  
✅ **Developer experience**: Type hints, IDE support, intuitive queries  
✅ **Flexibility**: Right tool for each task  

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