import ast

import pandas as pd

commits_filtered_df = pd.read_csv("data/apache_commons_lang_commits_filtered.csv")
commits_filtered_df["author_date"] = pd.to_datetime(commits_filtered_df["author_date"])
