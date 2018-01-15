import os

from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
database_path = os.path.join(project_dir, 'data', 'interim', 'relationships.sqlite')
base = declarative_base()


class RelationshipsSubmissions(base):
    """
    The primary key is chosen for uniqueness. One would think that ID is unique enough...
    """
    __tablename__ = 'relationships_submission'
    id = Column(String(12), nullable=False, primary_key=True)
    author = Column(String(20), nullable=False)
    created = Column(DateTime, nullable=False, primary_key=True)
    downs = Column(Integer, nullable=False)
    edited = Column(Boolean, nullable=False)
    edited_date = Column(Integer, nullable=False)
    locked = Column(Boolean, nullable=False)
    name = Column(String(10), nullable=False)
    num_comments = Column(Integer, nullable=False)
    permalink = Column(String(333), nullable=False)
    score = Column(Integer, nullable=False)
    # selftext = Column(, nullable=False)
    title = Column(String(300), nullable=False)
    ups = Column(Integer, nullable=False)


def create_tables(db_path):
    # Create an engine that stores data in the local directory's
    # sqlalchemy_example.db file.
    engine = create_engine('sqlite:///' + db_path)

    # Create all tables in the engine. This is equivalent to "Create Table"
    # statements in raw SQL.
    base.metadata.create_all(engine)


if __name__ == '__main__':
    create_tables(database_path)
