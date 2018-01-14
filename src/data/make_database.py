from collections import defaultdict
from datetime import datetime
import json
import logging
import os

from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logging.basicConfig(filename='make_database.log', level=logging.INFO)
logger = logging.getLogger(__name__)

project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
database_path = os.path.join(project_dir, 'data', 'interim', 'relationships.sqlite')
Base = declarative_base()


class Relationships_submissions(Base):
    __tablename__ = 'relationships_submission'
    id = Column(String(12), nullable=False, primary_key=True)
    author = Column(String(20), nullable=False)
    created = Column(DateTime, nullable=False, primary_key=True)
    downs = Column(Integer, nullable=False)
    edited = Column(Boolean, nullable=False)
    edited_delta = Column(Integer, nullable=False)
    locked = Column(Boolean, nullable=False)
    name = Column(String(10), nullable=False)
    num_comments = Column(Integer, nullable=False)
    permalink = Column(String(333), nullable=False)
    score = Column(Integer, nullable=False)
    # selftext = Column(, nullable=False)
    title = Column(String(300), nullable=False)
    ups = Column(Integer, nullable=False)


engine = create_engine('sqlite:///' + database_path)  # , echo=True)
Base.metadata.create_all(engine)
# Bind the engine to the metadata of the Base class so that the
# declaratives can be accessed through a DBSession instance
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
# A DBSession() instance establishes all conversations with the database
# and represents a "staging zone" for all the objects loaded into the
# database session object. Any change made against the objects in the
# session won't be persisted into the database until you call
# session.commit(). If you're not happy about the changes, you can
# revert all of them back to the last commit by calling
# session.rollback()
session = DBSession()
submissions_data_dir = os.path.join(project_dir, 'data', 'raw')

def str_to_dt(s:str):
    return datetime.fromtimestamp(int(s))

stats_typed = {
    'author': str, 'created': str_to_dt, 'downs': int, 'edited': bool, 'edited_delta': int, 'id': str, 'locked': bool,
    'name': str, 'num_comments': int, 'permalink': str, 'score': int, 'title': str, 'ups': int}

if __name__ == '__main__':
    d = defaultdict(int)
    for file in os.listdir(submissions_data_dir):
        if not file.endswith(".json"):
            continue
        filedate = file.replace('.json', '')

        with open(os.path.join(submissions_data_dir, file), 'r') as jsonfile:
            submission_list = json.load(jsonfile)

        sub_count = 0
        for count, submission in enumerate(submission_list):
            if not d[(submission['id'], submission['created'])]:
                d[(submission['id'], submission['created'])] = 1
            else:
                continue
            submission.pop('selftext', None)
            if submission['edited'] == 'False' or submission['edited']:
                submission['edited_delta'] = 0
                submission['edited'] = bool(submission['edited'])
            else:
                submission['edited_delta'] = submission['edited'][:-2]
                submission['edited'] = True
            submission['created'] = submission['created'][:-2]
            for stat, stype in stats_typed.items():
                submission[stat] = stype(submission[stat])
            new_submission = Relationships_submissions(**submission)
            session.add(new_submission)
            sub_count = count

        session.commit()

        logger.info(f'Date: {filedate}\tCount: {sub_count}')
