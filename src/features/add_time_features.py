# coding: utf-8
import sqlite3
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

plt.style.use('ggplot')

project_dir = Path(__file__).parents[2]
database_path = project_dir/'data'/'interim'/'relationships.sqlite'
dataframe_out_path = project_dir / 'data' / 'processed' / 'added_time_features.feather'

conn = sqlite3.connect(str(database_path))
df = pd.read_sql_query('select * from relationships_submission', conn, parse_dates=['created', 'edited_date'], index_col='created')

df = df.infer_objects()
df[['id', 'author', 'name', 'permalink', 'title']] = df[['id', 'author', 'name', 'permalink', 'title']].astype(str)

df['year'] = df.index.year
df['month'] = df.index.month
df['dayofyear'] = df.index.dayofyear
df['dayofweek'] = df.index.dayofweek
df['hour'] = df.index.hour
df['minute'] = df.index.minute
df['second'] = df.index.second
df['edited_delta'] = (df.edited_date - df.index)
df['edited_delta_min'] = df.edited_delta.dt.seconds/60
df['edited_delta_hour'] = df.edited_delta_min/60
df['edited_delta_day'] = df.edited_delta.dt.days
df = df[df.edited_delta_day < 100]

# feather.write_dataframe(df, str(dataframe_out_path))
# Feather doesnt currently support datetime... ugh.
df.to_pickle(dataframe_out_path.with_suffix('.pickle'))
print(f'Writing dataframe to file{str(dataframe_out_path.with_suffix(".pickle"))}')

