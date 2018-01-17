import json
import logging
from datetime import datetime, date
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from make_database import RelationshipsSubmissions, create_tables

logging.basicConfig(filename='populate_database.log', level=logging.INFO)
logger = logging.getLogger(__name__)

project_dir = Path(__file__).parents[1]
database_path = project_dir/'data'/'interim'/'relationships.sqlite'
submissions_data_dir = project_dir/'data'/'raw'

d = {}


def create_session(db_path):
    base = declarative_base()
    engine = create_engine('sqlite:///' + db_path)  # , echo=True)
    base.metadata.create_all(engine)
    # Bind the engine to the metadata of the base class so that the
    # declaratives can be accessed through a db_session instance
    base.metadata.bind = engine

    db_session = sessionmaker(bind=engine)
    # A db_session() instance establishes all conversations with the database
    # and represents a "staging zone" for all the objects loaded into the
    # database session object. Any change made against the objects in the
    # session won't be persisted into the database until you call
    # session.commit(). If you're not happy about the changes, you can
    # revert all of them back to the last commit by calling
    # session.rollback()
    return_session = db_session()
    return return_session


def str_to_dt(s: str):
    """
    Converts from a string timestamp to a datetime object
    :param s:
    :return:
    """
    return datetime.fromtimestamp(int(s.replace('.0', '')))


def format_submission(sub):
    """
    There are a few errors in the format from the PRAW data that need to be cleaned.
    More details in the comments
    :param sub:
    :return:
    """
    # There seems to be some redundancies somehow, not sure why, but this skips redundant info and logs a warning
    if d.get((sub['id'], sub['created']), False):
        logger.warning(f'Redundant Submission: {sub}')
        return None
    else:
        d[(sub['id'], sub['created'])] = 1

    sub.pop('selftext', None)

    # The edits have a format of 3 options: False, True, or timestamp
    if sub['edited'] == 'False' or sub['edited'] == 'True':
        sub['edited_date'] = sub['created']
        sub['edited'] = bool(sub['edited'] != 'False')
    else:
        sub['edited_date'] = sub['edited'].replace('.0', '')
        sub['edited'] = True

    # Trim creation to help conversion from str to date
    sub['created'] = sub['created'].replace('.0', '')

    # Apply type conversion
    for stat, stype in stats_typed.items():
        sub[stat] = stype(sub[stat])
    return sub


stats_typed = {
    'author': str, 'created': str_to_dt, 'downs': int, 'edited': bool, 'edited_date': str_to_dt, 'id': str,
    'locked': bool,
    'name': str, 'num_comments': int, 'permalink': str, 'score': int, 'title': str, 'ups': int}

if __name__ == '__main__':

    if not database_path.is_file():
        create_tables(database_path)
    session = create_session(database_path)

    for file in submissions_data_dir.iterdir():
        if not file.endswith(".json"):
            continue

        # This gives us good handling if we want to work on a subset of files based on date
        file_date = date(*[int(x) for x in str(file.stem).split('_')])
        if file_date < date(2017, 12, 31):
            continue

        with open(file, 'r') as json_file:
            submission_list = json.load(json_file)

        # Daily Count for logging
        sub_count = 0

        # Format and submit each submission to session
        for submission in submission_list:
            formatted_submission = format_submission(submission)
            if formatted_submission:
                new_submission = RelationshipsSubmissions(**formatted_submission)
                session.add(new_submission)
                sub_count += 1

        # Commit a days worth for efficiency
        session.commit()

        logger.info(f'Time: {datetime.now().time()}\tFile: {file.stem}\tCount: {sub_count}')
