# -*- coding: utf-8 -*-
import pandas as pd
import itertools
import praw
from pprint import pprint

reddit = praw.Reddit(client_id=auth['client_id'], client_secret=auth['client_secret'],
                     password=auth['password'], user_agent=auth['user_agent'],
                     username=auth['username'])
