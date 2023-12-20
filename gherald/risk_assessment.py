import ast
import re

import pandas as pd
from nltk.tokenize import word_tokenize
from pandarallel import pandarallel

pandarallel.initialize()


def read_filtered_commits(commits_file, filtered_commits_file):
    commits_df = pd.read_csv(commits_file)
    commits_df = commits_df[["id", "author_date", "author_email"]]
    commits_df["author_date_ts"] = commits_df["author_date"].apply(lambda x: int(pd.Timestamp(x).timestamp()))

    commits_filtered_df = pd.read_csv(filtered_commits_file)
    commits_filtered_df["author_date"] = pd.to_datetime(commits_filtered_df["author_date"])

    return commits_df, commits_filtered_df


def process_commits(commits_file, filtered_commits_file, commit_code_changes_file):
    code_df = pd.read_csv(commit_code_changes_file)
    code_df.code_changes = code_df.code_changes.apply(ast.literal_eval)

    valid_types = {"java"}

    def filter_code_lines(lines):
        filtered_lines = []
        multi_line_comment1 = 0
        multi_line_comment2 = 0
        for line_tuple in lines:
            line = line_tuple[1] if len(line_tuple) < 3 else line_tuple[2]
            line_num1 = line_tuple[0]
            line_num2 = 0 if len(line_tuple) < 3 else line_tuple[1]
            line = re.sub(r"\/\/.*", "", line)
            line = re.sub(r"\/\*((?!\*\/).)*\*\/", "", line)
            comment_start = re.search(r"\/\*.*", line)
            comment_end = re.search(r".*\*\/", line)
            if not comment_start and not comment_end:
                line = re.sub(r"^\s*\*.*", "", line)
            if comment_start:
                line = line[: comment_start.start()]
                if line_num1 > 0:
                    multi_line_comment1 = line_num1
                if line_num2 > 0:
                    multi_line_comment2 = line_num2
            if comment_end:
                line = line[comment_end.end() :]
                if line_num1 > 0:
                    multi_line_comment1 = 0
                if line_num2 > 0:
                    multi_line_comment2 = 0
            if multi_line_comment1 > 0 and multi_line_comment1 + 1 == line_num1:
                multi_line_comment1 += 1
                continue
            if multi_line_comment2 > 0 and multi_line_comment2 + 1 == line_num2:
                multi_line_comment2 += 1
                continue
            tokenized_line = word_tokenize(line)
            if len(tokenized_line) > 0:
                filtered_lines.append(tokenized_line)
        return filtered_lines

    def get_additions(x):
        additions = 0
        for f in x:
            file_type = f["filename"].split(".")[-1]
            if "test" not in f["filename"] and file_type in valid_types:
                added_code = filter_code_lines(f["added_code"])
                additions += len(added_code)
        return additions

    def get_deletions(x):
        deletions = 0
        for f in x:
            file_type = f["filename"].split(".")[-1]
            if "test" not in f["filename"] and file_type in valid_types:
                deleted_code = filter_code_lines(f["removed_code"])
                deletions += len(deleted_code)
        return deletions

    def get_filenames(x):
        files = []
        for f in x:
            files.append(f["filename"])
        return files

    code_df["additions"] = code_df["code_changes"].parallel_apply(lambda x: get_additions(x))
    code_df["deletions"] = code_df["code_changes"].parallel_apply(lambda x: get_deletions(x))

    commits_filtered_df = read_filtered_commits(commits_file, filtered_commits_file)

    code_df = code_df[["id", "additions", "deletions"]]
    commits_filtered_df = commits_filtered_df.drop(columns=["additions", "deletions"])
    commits_filtered_df = pd.merge(commits_filtered_df, code_df, how="left", on="id")
