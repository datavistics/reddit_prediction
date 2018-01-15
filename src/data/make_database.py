from collections import defaultdict
from datetime import datetime, date
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
database_path = os.path.join(project_dir, 'data', 'interim', 'relationships_fix.sqlite')
Base = declarative_base()


class Relationships_submissions(Base):
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
d = defaultdict(int)

def str_to_dt(s:str):
    return datetime.fromtimestamp(int(s))

stats_typed = {
    'author': str, 'created': str_to_dt, 'downs': int, 'edited': bool, 'edited_date': str_to_dt, 'id': str, 'locked': bool,
    'name': str, 'num_comments': int, 'permalink': str, 'score': int, 'title': str, 'ups': int}


def format_submission(submission):
    # There seems to be some redundancies somehow, not sure why, but this skips redundant info and logs a warning
    if not d[(submission['id'], submission['created'])]:
        d[(submission['id'], submission['created'])] = 1
        logger.warning(f'Redundant Submission: {submission}')
    else:
        return None
    submission.pop('selftext', None)

    # The edits have a format of 3 options: False, True, or timestamp
    if submission['edited'] == 'False' or submission['edited'] == 'True':
        submission['edited_date'] = submission['created']
        submission['edited'] = bool(submission['edited'] != 'False')
    else:
        submission['edited_date'] = submission['edited'].replace('.0', '')
        submission['edited'] = True

    # Trim creation to help conversion from str to date
    submission['created'] = submission['created'].replace('.0', '')

    # Apply type conversion
    for stat, stype in stats_typed.items():
        submission[stat] = stype(submission[stat])
    return submission


if __name__ == '__main__':
    for file in os.listdir(submissions_data_dir):
        if not file.endswith(".json"):
            continue

        # This gives us good handling if we want to work on a subset of files based on date
        filename = file.replace('.json', '')
        file_date = date(*[int(x) for x in filename.split('_')])
        if file_date < date(2016, 3, 5):
            continue

        with open(os.path.join(submissions_data_dir, file), 'r') as json_file:
            submission_list = json.load(json_file)

        # Daily Count for logging
        sub_count = 0

        # Format and submit each submission to session
        for count, submission in enumerate(submission_list):
            formatted_submission = format_submission(submission)
            if formatted_submission:
                new_submission = Relationships_submissions(**formatted_submission)
            session.add(new_submission)
            sub_count = count

        # Commit a days worth for efficiency
        session.commit()

        logger.info(f'Date: {filedate}\tCount: {sub_count}')
