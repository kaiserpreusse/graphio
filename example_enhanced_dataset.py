#!/usr/bin/env python3
"""
Example demonstrating the enhanced dataset functionality:
- Using Person.dataset() to get NodeSet 
- Using Person.works_at.dataset() to get RelationshipSet
- Adding OGM instances directly to datasets
"""

from graphio import NodeModel, Relationship
from neo4j import GraphDatabase

# Define OGM models
class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']
    name: str
    email: str
    age: int
    
    # Relationship definitions
    knows: Relationship = Relationship('Person', 'KNOWS', 'Person')
    works_at: Relationship = Relationship('Person', 'WORKS_AT', 'Company')

class Company(NodeModel):
    _labels = ['Company']
    _merge_keys = ['name']
    name: str
    industry: str

def main():
    # Connect to Neo4j (adjust credentials as needed)
    driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
    
    # Create OGM instances with validation
    alice = Person(name='Alice Smith', email='alice@example.com', age=30)
    bob = Person(name='Bob Johnson', email='bob@example.com', age=25)
    charlie = Person(name='Charlie Brown', email='charlie@example.com', age=35)
    
    acme = Company(name='Acme Inc', industry='Technology')
    beta = Company(name='Beta Corp', industry='Finance')
    
    print("✅ Created OGM instances with validation")
    
    # Get datasets using the new enhanced API
    people = Person.dataset()          # Returns NodeSet
    companies = Company.dataset()      # Returns NodeSet
    employment = Person.works_at.dataset()  # Returns RelationshipSet
    friendships = Person.knows.dataset()   # Returns RelationshipSet
    
    print("✅ Got datasets from OGM classes")
    
    # Add OGM instances directly to datasets (no dict conversion needed!)
    # Using the new cleaner .add() method
    people.add(alice)
    people.add(bob)  
    people.add(charlie)
    
    companies.add(acme)
    companies.add(beta)
    
    print("✅ Added OGM instances to NodeSets")
    
    # Add relationships using OGM instances with cleaner .add() method
    employment.add(alice, acme, {'role': 'Engineer', 'start_date': '2023-01-01'})
    employment.add(bob, acme, {'role': 'Designer', 'start_date': '2023-02-01'})  
    employment.add(charlie, beta, {'role': 'Manager', 'start_date': '2022-06-01'})
    
    friendships.add(alice, bob, {'since': '2020', 'strength': 0.8})
    friendships.add(bob, charlie, {'since': '2021', 'strength': 0.6})
    
    print("✅ Added relationships using OGM instances")
    
    # Bulk create in Neo4j
    try:
        people.create(driver)
        print(f"✅ Created {len(people.nodes)} people in Neo4j")
        
        companies.create(driver) 
        print(f"✅ Created {len(companies.nodes)} companies in Neo4j")
        
        employment.create(driver)
        print(f"✅ Created {len(employment.relationships)} employment relationships in Neo4j")
        
        friendships.create(driver)
        print(f"✅ Created {len(friendships.relationships)} friendship relationships in Neo4j")
        
        print("\n🎉 All data successfully loaded using enhanced dataset API!")
        
        # Demonstrate mixing OGM instances and dicts
        print("\n--- Demonstrating mixed usage ---")
        more_people = Person.dataset()
        
        # Add OGM instance
        diana = Person(name='Diana Prince', email='diana@example.com', age=28)
        more_people.add(diana)
        
        # Add dict (still works!)
        more_people.add({'name': 'Eve Adams', 'email': 'eve@example.com', 'age': 32})
        
        more_people.create(driver)
        print("✅ Successfully mixed OGM instances and dicts in same NodeSet")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure Neo4j is running and credentials are correct")
    finally:
        driver.close()

if __name__ == '__main__':
    main()