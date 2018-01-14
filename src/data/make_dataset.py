# -*- coding: utf-8 -*-
import json
import logging
import os
from datetime import datetime, timedelta

import praw

logging.basicConfig(filename='make_dataset.log', level=logging.INFO)
logger = logging.getLogger(__name__)


with open('authentication.json') as file:
    auth = json.load(file)

# https://www.reddit.com/prefs/apps/
reddit = praw.Reddit(client_id=auth['client_id'], client_secret=auth['client_secret'],
                     password=auth['password'], user_agent=auth['user_agent'],
                     username=auth['username'])
reddit.read_only = True
sub = reddit.subreddit('relationships')
stats_to_grab = ['author', 'created', 'downs', 'edited', 'id', 'locked', 'name', 'num_comments', 'permalink', 'score', 'selftext',
                 'title', 'ups']

end = datetime(2018, 1, 13)
start = datetime(2008, 7, 10)

for time_delta in range((end - start).days):
    daily_submissions = []
    num_of_sub_per_day = 0
    sub_start = end - timedelta(days=time_delta + 1)
    sub_end = end - timedelta(days=time_delta)
    for submission in sub.submissions(sub_start.timestamp(), sub_end.timestamp()):
        d = {}
        num_of_sub_per_day += 1
        for stat in stats_to_grab:
            d[stat] = str(submission.__getattribute__(stat))
        daily_submissions.append(d)
    logger.info(f"Submission {sub_start.date()}:\t {num_of_sub_per_day}")
    filename = f'{sub_start.date()}.json'.replace('-', '_')
    filepath = os.path.join(os.getcwd(), os.pardir, os.pardir, 'data', 'raw', filename)
    with open(filepath, 'w') as outfile:
        json.dump(daily_submissions, outfile)
