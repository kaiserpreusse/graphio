import logging
from neo4j import GraphDatabase
from model import Base, User, Post


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    # Configure Neo4j connection
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "testtest"  # Replace with your actual password

    logger.info("Connecting to Neo4j database...")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # Initialize the Base with the driver
    Base.initialize().set_driver(driver)

    try:
        # Create indexes for all models
        logger.info("Creating indexes...")
        Base.model_create_index()

        # Create some users
        logger.info("Creating users...")
        alice = User(name="Alice Smith", email="alice@example.com", age=32)
        bob = User(name="Bob Johnson", email="bob@example.com", age=28)
        charlie = User(name="Charlie Brown", email="charlie@example.com", age=35)

        # Create some posts
        logger.info("Creating posts...")
        post1 = Post(id="post-001", title="Hello Neo4j",
                     content="This is my first post about graph databases")
        post2 = Post(id="post-002", title="Graph Models",
                     content="Modeling data with Neo4j is powerful")

        # Set up relationships
        logger.info("Setting up relationships...")
        alice.friends.add(bob)
        bob.friends.add(charlie)

        post1.author.add(alice)
        post2.author.add(bob)

        # Use the Graph helper to create nodes and relationships
        logger.info("Persisting to database...")
        # graph = Graph()
        # graph.merge(alice, bob, charlie, post1, post2)
        alice.merge()

        # Verify data was created
        logger.info("Verifying data creation...")
        users = User.match(name="Alice Smith")
        if users:
            logger.info(f"Found user: {users[0].name} with email {users[0].email}")

            # Find Alice's friends
            friends = users[0].friends.match()
            logger.info(f"{users[0].name} has {len(friends)} friends")
            for friend in friends:
                logger.info(f"Friend: {friend.name}")

        logger.info("Application completed successfully")

    except Exception as e:
        raise

    finally:
        driver.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()