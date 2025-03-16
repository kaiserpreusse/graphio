from graphio import declarative_base, Relationship

# Create the base class for all your models
Base = declarative_base()


# Define a User node model with relationships
class User(Base.NodeModel):
    _labels = ['User']
    _merge_keys = ['email']

    name: str
    email: str
    age: int = None

    # Define a self-referential relationship
    friends:Relationship = Relationship(source='User', rel_type='FRIENDS_WITH', target='User')


# Define a Post node model
class Post(Base.NodeModel):
    _labels = ['Post']
    _merge_keys = ['id']

    id: str
    title: str
    content: str
    created_at: str = None

    # Relationship to the author
    author:Relationship = Relationship(source='User', rel_type='AUTHORED', target='Post')
