import ast
import re

import numpy as np
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


def read_commit_code(commit_code_changes_file):
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

    code_df["additions"] = code_df["code_changes"].parallel_apply(lambda x: get_additions(x))
    code_df["deletions"] = code_df["code_changes"].parallel_apply(lambda x: get_deletions(x))
    code_df = code_df[["id", "additions", "deletions"]]
    return code_df


def preprocess_data(commits_df, commits_filtered_df, bug_inducing_commit_file, commit_modified_files):
    bug_inducing_commits = pd.read_csv(bug_inducing_commit_file)

    def get_inducing_commits(obj):
        obj = ast.literal_eval(obj)
        bug_inducing_ids = {}
        for file, ids in obj.items():
            for value in ids:
                id = value[0]
                line_num = value[1]
                if id not in bug_inducing_ids:
                    bug_inducing_ids[id] = {}
                if file not in bug_inducing_ids[id]:
                    bug_inducing_ids[id][file] = []
                bug_inducing_ids[id][file].append(line_num)
        return bug_inducing_ids.items()

    bug_inducing_commits["id"] = bug_inducing_commits["id"].apply(lambda x: get_inducing_commits(x))

    bug_inducing_commits = bug_inducing_commits.explode("id")
    bug_inducing_commits = bug_inducing_commits.dropna(subset=["id"])
    bug_inducing_commits = bug_inducing_commits.reset_index(drop=True)
    bug_inducing_commits["file"] = bug_inducing_commits["id"].apply(lambda x: x[1])
    bug_inducing_commits["id"] = bug_inducing_commits["id"].apply(lambda x: x[0])

    bug_inducing_commits["file"] = bug_inducing_commits["file"].apply(lambda x: ast.literal_eval(str(x)))
    bug_inducing_commits["file"] = bug_inducing_commits["file"].apply(lambda x: x.items())
    bug_inducing_commits = bug_inducing_commits.explode("file")
    bug_inducing_commits = bug_inducing_commits.dropna(subset=["file"])
    bug_inducing_commits = bug_inducing_commits.reset_index(drop=True)
    bug_inducing_commits["line_num"] = bug_inducing_commits["file"].apply(lambda x: x[1])
    bug_inducing_commits["file"] = bug_inducing_commits["file"].apply(lambda x: x[0])

    bug_inducing_commits = pd.merge(bug_inducing_commits, commits_df, how="left", on="id")
    bug_inducing_files = bug_inducing_commits

    commit_file_method = pd.read_csv(commit_modified_files)
    commit_file_method = commit_file_method[["id", "file", "file_complexity", "methods", "file_oldpaths"]]
    commit_file_method["methods"] = commit_file_method["methods"].apply(lambda x: ast.literal_eval(str(x)))
    commit_file_method["file_oldpaths"] = commit_file_method["file_oldpaths"].apply(lambda x: ast.literal_eval(str(x)))
    commit_file_method = commit_file_method.rename(columns={"id": "bug_fixing_commit_id"})

    bug_inducing_commits = pd.merge(
        bug_inducing_commits, commit_file_method, how="left", on=["bug_fixing_commit_id", "file"]
    )
    bug_inducing_commits = bug_inducing_commits.dropna(subset=["methods"])
    bug_inducing_commits = bug_inducing_commits.rename(columns={"methods": "bug_fixing_commit_methods"})
    commit_file_method = commit_file_method.rename(columns={"bug_fixing_commit_id": "id"})
    bug_inducing_commits = pd.merge(
        bug_inducing_commits, commit_file_method[["id", "file", "methods"]], how="left", on=["id", "file"]
    )
    bug_inducing_commits = bug_inducing_commits.dropna(subset=["methods"])
    bug_inducing_commits = bug_inducing_commits.rename(columns={"methods": "bug_inducing_commit_methods"})

    def get_modified_methods(commit):
        line_nums = commit["line_num"]
        methods = commit["bug_fixing_commit_methods"]
        modified_methods = []
        for line_num in line_nums:
            for method in methods:
                method_name = method[0]
                method_long_name = method[1]
                start_line = method[2]
                end_line = method[3]
                if line_num >= start_line and line_num <= end_line:
                    modified_methods.append(method_long_name)
        return modified_methods

    def get_method_name(methods):
        method_names = []
        for method in methods:
            method_long_name = method[1]
            method_names.append(method_long_name)
        return method_names

    bug_inducing_commits["bug_fixing_commit_methods"] = bug_inducing_commits.apply(
        lambda x: get_modified_methods(x), axis=1
    )
    bug_inducing_commits["bug_inducing_commit_methods"] = bug_inducing_commits["bug_inducing_commit_methods"].apply(
        lambda x: get_method_name(x)
    )

    def get_bug_inducing_methods(row):
        bug_fixing_commit_methods = row["bug_fixing_commit_methods"]
        bug_inducing_commit_methods = row["bug_inducing_commit_methods"]
        return list(set(bug_fixing_commit_methods).intersection(bug_inducing_commit_methods))

    bug_inducing_commits["methods"] = bug_inducing_commits.apply(lambda row: get_bug_inducing_methods(row), axis=1)
    bug_inducing_commits = bug_inducing_commits.explode("methods")
    bug_inducing_commits = bug_inducing_commits.dropna(subset=["methods"])
    bug_inducing_commits = bug_inducing_commits.reset_index(drop=True)
    bug_inducing_commits = bug_inducing_commits.rename(columns={"methods": "method"})
    bug_inducing_methods = bug_inducing_commits

    commits_filtered_df_files_expanded = pd.merge(commits_filtered_df, commit_file_method, how="left", on=["id"])
    commits_filtered_df_methods_expanded = commits_filtered_df_files_expanded.explode("methods")
    commits_filtered_df_methods_expanded = commits_filtered_df_methods_expanded.reset_index(drop=True)
    commits_filtered_df_methods_expanded = commits_filtered_df_methods_expanded.rename(columns={"methods": "method"})
    commits_filtered_df_methods_expanded = commits_filtered_df_methods_expanded.dropna(subset=["method"])
    commits_filtered_df_methods_expanded["method_start_line"] = commits_filtered_df_methods_expanded["method"].apply(
        lambda x: x[2]
    )
    commits_filtered_df_methods_expanded["method_end_line"] = commits_filtered_df_methods_expanded["method"].apply(
        lambda x: x[3]
    )
    commits_filtered_df_methods_expanded["method_complexity"] = commits_filtered_df_methods_expanded["method"].apply(
        lambda x: x[4]
    )
    commits_filtered_df_methods_expanded["method"] = commits_filtered_df_methods_expanded["method"].apply(
        lambda x: x[1]
    )

    commits_df_files_expanded = pd.merge(commits_df, commit_file_method, how="left", on=["id"])
    commits_df_methods_expanded = commits_df_files_expanded.explode("methods")
    commits_df_methods_expanded = commits_df_methods_expanded.reset_index(drop=True)
    commits_df_methods_expanded = commits_df_methods_expanded.rename(columns={"methods": "method"})
    commits_df_methods_expanded = commits_df_methods_expanded.dropna(subset=["method"])
    commits_df_methods_expanded["method"] = commits_df_methods_expanded["method"].apply(lambda x: x[1])

    data_df = commits_filtered_df_files_expanded[
        [
            "id",
            "author_email",
            "author_date",
            "author_date_ts",
            "additions",
            "deletions",
            "file",
            "file_complexity",
            "file_oldpaths",
            "methods",
        ]
    ]
    data_df = data_df.dropna(subset=["file_complexity"])
    data_df = data_df.reset_index(drop=True)

    def is_valid_type(x):
        if "test" in x.lower() or x.split(".")[-1] != "java":
            return False
        return True

    data_df["is_valid_type"] = data_df["file"].apply(lambda x: is_valid_type(x))
    data_df = data_df[data_df["is_valid_type"]]

    return bug_inducing_files, bug_inducing_methods, commits_df_files_expanded, commits_df_methods_expanded, data_df


def risk_assessment(
    commits_file, filtered_commits_file, commit_code_changes_file, bug_inducing_commit_file, commit_modified_files
):
    commits_df, commits_filtered_df = read_filtered_commits(commits_file, filtered_commits_file)
    code_df = read_commit_code(commit_code_changes_file)
    commits_filtered_df = commits_filtered_df.drop(columns=["additions", "deletions"])
    commits_filtered_df = pd.merge(commits_filtered_df, code_df, how="left", on="id")
    (
        bug_inducing_files,
        bug_inducing_methods,
        commits_df_files_expanded,
        commits_df_methods_expanded,
        data_df,
    ) = preprocess_data(commits_df, commits_filtered_df, bug_inducing_commit_file, commit_modified_files)

    def get_author_prior_changes(row):
        author_email = row["author_email"]
        author_date_ts = row["author_date_ts"]
        commit_ids = set(
            commits_df[(author_email == commits_df["author_email"]) & (commits_df["author_date_ts"] < author_date_ts)][
                "id"
            ]
        )
        return len(commit_ids)

    def get_author_recent_changes(row):
        author_email = row["author_email"]
        author_date_ts = row["author_date_ts"]
        author_dates_ts = list(
            commits_df[(author_email == commits_df["author_email"]) & (commits_df["author_date_ts"] < author_date_ts)][
                "author_date_ts"
            ]
        )
        count = 0
        for date_ts in author_dates_ts:
            age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
            count += 1 / (1 + age)
        return count

    def get_author_prior_bugs(row):
        author_email = row["author_email"]
        author_date_ts = row["author_date_ts"]
        commit_ids = set(
            bug_inducing_files[
                (author_email == bug_inducing_files["author_email"])
                & (bug_inducing_files["author_date_ts"] < author_date_ts)
            ]["id"]
        )
        return len(commit_ids)

    def get_author_recent_bugs(row):
        author_email = row["author_email"]
        author_date_ts = row["author_date_ts"]
        bugs_df = bug_inducing_files[
            (author_email == bug_inducing_files["author_email"])
            & (bug_inducing_files["author_date_ts"] < author_date_ts)
        ][["id", "author_date_ts"]]
        bugs_df = bugs_df.drop_duplicates()
        author_dates_ts = list(bugs_df["author_date_ts"])
        count = 0
        for date_ts in author_dates_ts:
            age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
            count += 1 / (1 + age)
        return count

    def get_max_author_prior_changes(row):
        author_date_ts = row["author_date_ts"]
        author_prior_changes_max = data_df[
            (data_df["author_date_ts"] <= author_date_ts) & (data_df["author_date_ts"] > author_date_ts - 31557600)
        ]["author_prior_changes"].max()
        return author_prior_changes_max

    def get_max_author_recent_changes(row):
        author_date_ts = row["author_date_ts"]
        author_prior_changes_max = data_df[
            (data_df["author_date_ts"] <= author_date_ts) & (data_df["author_date_ts"] > author_date_ts - 31557600)
        ]["author_recent_changes"].max()
        return author_prior_changes_max

    data_df["author_prior_changes"] = data_df.parallel_apply(lambda row: get_author_prior_changes(row), axis=1)
    data_df["author_recent_changes"] = data_df.parallel_apply(lambda row: get_author_recent_changes(row), axis=1)
    data_df["author_prior_bugs"] = data_df.parallel_apply(lambda row: get_author_prior_bugs(row), axis=1)
    data_df["author_recent_bugs"] = data_df.parallel_apply(lambda row: get_author_recent_bugs(row), axis=1)
    data_df["author_max_prior_changes"] = data_df.parallel_apply(lambda row: get_max_author_prior_changes(row), axis=1)
    data_df["author_max_recent_changes"] = data_df.parallel_apply(
        lambda row: get_max_author_recent_changes(row), axis=1
    )
    data_df["author_prior_change_score"] = np.log(data_df["author_prior_changes"] + 1) / np.log(
        data_df["author_max_prior_changes"] + 1
    )
    data_df["author_recent_change_score"] = np.log(data_df["author_recent_changes"] + 1) / np.log(
        data_df["author_max_recent_changes"] + 1
    )
    data_df = data_df.fillna(0.0)

    def get_file_prior_changes(row):
        file = row["file"]
        author_date_ts = row["author_date_ts"]
        commit_ids = set(
            commits_df_files_expanded[
                (file == commits_df_files_expanded["file"])
                & (commits_df_files_expanded["author_date_ts"] < author_date_ts)
            ]["id"]
        )
        file_oldpaths = row["file_oldpaths"]
        for f in file_oldpaths:
            ids = set(
                commits_df_files_expanded[
                    (f == commits_df_files_expanded["file"])
                    & (commits_df_files_expanded["author_date_ts"] < author_date_ts)
                ]["id"]
            )
            commit_ids.update(ids)
        return len(commit_ids)

    def get_file_prior_bugs(row):
        file = row["file"]
        author_date_ts = row["author_date_ts"]
        commit_ids = set(
            bug_inducing_files[
                (file == bug_inducing_files["file"]) & (bug_inducing_files["author_date_ts"] < author_date_ts)
            ]["id"]
        )
        file_oldpaths = row["file_oldpaths"]
        for f in file_oldpaths:
            ids = set(
                bug_inducing_files[
                    (f == bug_inducing_files["file"]) & (bug_inducing_files["author_date_ts"] < author_date_ts)
                ]["id"]
            )
            commit_ids.update(ids)
        return len(commit_ids)

    def get_file_recent_changes(row):
        file = row["file"]
        author_date_ts = row["author_date_ts"]
        changes_df = commits_df_files_expanded[
            (file == commits_df_files_expanded["file"]) & (commits_df_files_expanded["author_date_ts"] < author_date_ts)
        ][["id", "author_date_ts"]]
        changes_df = changes_df.drop_duplicates()
        author_dates_ts = list(changes_df["author_date_ts"])
        count = 0
        for date_ts in author_dates_ts:
            age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
            count += 1 / (1 + age)
        file_oldpaths = row["file_oldpaths"]
        for f in file_oldpaths:
            changes_df = commits_df_files_expanded[
                (f == commits_df_files_expanded["file"])
                & (commits_df_files_expanded["author_date_ts"] < author_date_ts)
            ][["id", "author_date_ts"]]
            changes_df = changes_df.drop_duplicates()
            author_dates_ts = list(changes_df["author_date_ts"])
            for date_ts in author_dates_ts:
                age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
                count += 1 / (1 + age)
        return count

    def get_file_recent_bugs(row):
        file = row["file"]
        author_date_ts = row["author_date_ts"]
        bugs_df = bug_inducing_files[
            (file == bug_inducing_files["file"]) & (bug_inducing_files["author_date_ts"] < author_date_ts)
        ][["id", "author_date_ts"]]
        bugs_df = bugs_df.drop_duplicates()
        author_dates_ts = list(bugs_df["author_date_ts"])
        count = 0
        for date_ts in author_dates_ts:
            age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
            count += 1 / (1 + age)
        file_oldpaths = row["file_oldpaths"]
        for f in file_oldpaths:
            bugs_df = bug_inducing_files[
                (f == bug_inducing_files["file"]) & (bug_inducing_files["author_date_ts"] < author_date_ts)
            ][["id", "author_date_ts"]]
            bugs_df = bugs_df.drop_duplicates()
            author_dates_ts = list(bugs_df["author_date_ts"])
            for date_ts in author_dates_ts:
                age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
                count += 1 / (1 + age)
        return count

    data_df["file_prior_changes"] = data_df.parallel_apply(lambda row: get_file_prior_changes(row), axis=1)
    data_df["file_prior_bugs"] = data_df.parallel_apply(lambda row: get_file_prior_bugs(row), axis=1)
    data_df["file_recent_changes"] = data_df.parallel_apply(lambda row: get_file_recent_changes(row), axis=1)
    data_df["file_recent_bugs"] = data_df.parallel_apply(lambda row: get_file_recent_bugs(row), axis=1)
    data_df = data_df.fillna(0.0)

    def get_author_file_prior_changes(row):
        file = row["file"]
        author_date_ts = row["author_date_ts"]
        author_email = row["author_email"]
        commit_ids = set(
            commits_df_files_expanded[
                (file == commits_df_files_expanded["file"])
                & (author_email == commits_df_files_expanded["author_email"])
                & (commits_df_files_expanded["author_date_ts"] < author_date_ts)
            ]["id"]
        )
        file_oldpaths = row["file_oldpaths"]
        for f in file_oldpaths:
            ids = set(
                commits_df_files_expanded[
                    (f == commits_df_files_expanded["file"])
                    & (author_email == commits_df_files_expanded["author_email"])
                    & (commits_df_files_expanded["author_date_ts"] < author_date_ts)
                ]["id"]
            )
            commit_ids.update(ids)
        return len(commit_ids)

    data_df["author_file_prior_changes"] = data_df.parallel_apply(
        lambda row: get_author_file_prior_changes(row), axis=1
    )
    author_file_prior_changes_df = data_df.groupby(["id"]).agg(
        {"author_file_prior_changes": "sum", "file_prior_changes": "sum"}
    )
    author_file_prior_changes_df.columns = ["author_file_prior_changes", "file_prior_changes"]
    author_file_prior_changes_df = author_file_prior_changes_df.reset_index()
    author_file_prior_changes_df["author_file_awareness"] = np.log(
        author_file_prior_changes_df["author_file_prior_changes"] + 1
    ) / np.log(author_file_prior_changes_df["file_prior_changes"] + 1)
    author_file_prior_changes_df["author_file_awareness"] = author_file_prior_changes_df[
        "author_file_awareness"
    ].fillna(0)
    data_df = data_df.drop(columns=["author_file_prior_changes"])
    data_df = pd.merge(
        data_df,
        author_file_prior_changes_df[["id", "author_file_prior_changes", "author_file_awareness"]],
        how="left",
        on="id",
    )

    def get_max_author_file_awareness(row):
        author_date_ts = row["author_date_ts"]
        author_file_awareness_max = data_df[
            (data_df["author_date_ts"] <= author_date_ts) & (data_df["author_date_ts"] > author_date_ts - 15552000)
        ]["author_file_awareness"].max()
        return author_file_awareness_max

    data_df["author_max_file_awareness"] = data_df.parallel_apply(
        lambda row: get_max_author_file_awareness(row), axis=1
    )
    data_df["author_file_awareness_score"] = data_df["author_file_awareness"] / data_df["author_max_file_awareness"]
    data_df["author_file_awareness_score"] = data_df["author_file_awareness_score"].fillna(0)

    def get_author_risk_score(row):
        author_prior_change_score = row["author_prior_change_score"]
        author_recent_change_score = row["author_recent_change_score"]
        author_file_awareness = row["author_file_awareness"]
        author_score = 1 - (author_prior_change_score + author_recent_change_score + author_file_awareness) / 3
        return author_score

    data_df["author_risk_score"] = data_df.apply(lambda row: get_author_risk_score(row), axis=1)

    def get_file_risk_score(row):
        file_recent_changes = row["file_recent_changes"]
        file_recent_bugs = row["file_recent_bugs"]
        file_score = (file_recent_bugs + 1) / (file_recent_changes - file_recent_bugs + 1)
        factor = np.log(file_recent_changes + 2)
        return factor * file_score

    data_df["file_risk_score"] = data_df.apply(lambda row: get_file_risk_score(row), axis=1)
    file_risk_scores_df = data_df.groupby(["id"]).agg({"file_risk_score": "mean"})
    file_risk_scores_df.columns = ["file_risk_score_sum"]
    file_risk_scores_df = file_risk_scores_df.reset_index()
    data_df = pd.merge(data_df, file_risk_scores_df[["id", "file_risk_score_sum"]], how="left", on="id")

    def get_recent_max_file_risk_score(row):
        author_date_ts = row["author_date_ts"]
        file_risk_score_max = data_df[
            (data_df["author_date_ts"] <= author_date_ts) & (data_df["author_date_ts"] > author_date_ts - 15552000)
        ]["file_risk_score_sum"].quantile(0.99)
        return file_risk_score_max

    data_df["recent_file_risk_score_max"] = data_df.parallel_apply(
        lambda row: get_recent_max_file_risk_score(row), axis=1
    )
    data_df["file_risk_score_relative"] = data_df["file_risk_score_sum"] / data_df["recent_file_risk_score_max"]
    data_df["file_risk_score_relative"] = data_df["file_risk_score_relative"].apply(lambda x: x if x < 1 else 1)
    data_file_df = data_df

    data_method_df = data_df
    data_method_df = data_method_df.explode("methods")
    data_method_df = data_method_df.reset_index(drop=True)
    data_method_df = data_method_df.rename(columns={"methods": "method"})
    data_method_df = data_method_df.dropna(subset=["method"])

    data_method_df["method_start_line"] = data_method_df["method"].apply(lambda x: x[2])
    data_method_df["method_end_line"] = data_method_df["method"].apply(lambda x: x[3])
    data_method_df["method_complexity"] = data_method_df["method"].apply(lambda x: x[4])
    data_method_df["method"] = data_method_df["method"].apply(lambda x: x[1])

    def get_method_prior_changes(row):
        method = row["method"]
        author_date_ts = row["author_date_ts"]
        commit_ids = set(
            commits_df_methods_expanded[
                (method == commits_df_methods_expanded["method"])
                & (commits_df_methods_expanded["author_date_ts"] < author_date_ts)
            ]["id"]
        )
        return len(commit_ids)

    def get_method_prior_bugs(row):
        method = row["method"]
        author_date_ts = row["author_date_ts"]
        commit_ids = set(
            bug_inducing_methods[
                (method == bug_inducing_methods["method"]) & (bug_inducing_methods["author_date_ts"] < author_date_ts)
            ]["id"]
        )
        return len(commit_ids)

    def get_method_recent_changes(row):
        method = row["method"]
        author_date_ts = row["author_date_ts"]
        changes_df = commits_df_methods_expanded[
            (method == commits_df_methods_expanded["method"])
            & (commits_df_methods_expanded["author_date_ts"] < author_date_ts)
        ][["id", "author_date_ts"]]
        changes_df = changes_df.drop_duplicates()
        author_dates_ts = list(changes_df["author_date_ts"])
        count = 0
        for date_ts in author_dates_ts:
            age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
            count += 1 / (1 + age)
        return count

    def get_method_recent_bugs(row):
        method = row["method"]
        author_date_ts = row["author_date_ts"]
        bugs_df = bug_inducing_methods[
            (method == bug_inducing_methods["method"]) & (bug_inducing_methods["author_date_ts"] < author_date_ts)
        ][["id", "author_date_ts"]]
        bugs_df = bugs_df.drop_duplicates()
        author_dates_ts = list(bugs_df["author_date_ts"])
        count = 0
        for date_ts in author_dates_ts:
            age = int((author_date_ts - date_ts) / (60 * 60 * 24 * 365.25))
            count += 1 / (1 + age)
        return count

    data_method_df["method_prior_changes"] = data_method_df.parallel_apply(
        lambda row: get_method_prior_changes(row), axis=1
    )
    data_method_df["method_prior_bugs"] = data_method_df.parallel_apply(lambda row: get_method_prior_bugs(row), axis=1)
    data_method_df["method_recent_changes"] = data_method_df.parallel_apply(
        lambda row: get_method_recent_changes(row), axis=1
    )
    data_method_df["method_recent_bugs"] = data_method_df.parallel_apply(
        lambda row: get_method_recent_bugs(row), axis=1
    )
    data_method_df = data_method_df.fillna(0.0)

    def get_method_score(row):
        method_recent_changes = row["method_recent_changes"]
        method_recent_bugs = row["method_recent_bugs"]
        method_score = (method_recent_bugs + 1) / (method_recent_changes - method_recent_bugs + 1)
        factor = np.log(method_recent_changes + 2)
        return factor * method_score

    data_method_df["method_risk_score"] = data_method_df.apply(lambda row: get_method_score(row), axis=1)
    method_risk_scores_df = data_method_df.groupby(["id"]).agg({"method_risk_score": "mean"})
    method_risk_scores_df.columns = ["method_risk_score_sum"]
    method_risk_scores_df = method_risk_scores_df.reset_index()
    data_method_df = pd.merge(
        data_method_df, method_risk_scores_df[["id", "method_risk_score_sum"]], how="left", on="id"
    )

    def get_recent_max_method_risk_score(row):
        author_date_ts = row["author_date_ts"]
        method_risk_score_max = data_method_df[
            (data_method_df["author_date_ts"] <= author_date_ts)
            & (data_method_df["author_date_ts"] > author_date_ts - 15552000)
        ]["method_risk_score_sum"].quantile(0.99)
        return method_risk_score_max

    data_method_df["recent_method_risk_score_max"] = data_method_df.parallel_apply(
        lambda row: get_recent_max_method_risk_score(row), axis=1
    )
    data_method_df["method_risk_score_relative"] = (
        data_method_df["method_risk_score_sum"] / data_method_df["recent_method_risk_score_max"]
    )
    data_method_df["method_risk_score_relative"] = data_method_df["method_risk_score_relative"].apply(
        lambda x: x if x < 1 else 1
    )

    return data_file_df, data_method_df


def prepare_experiment_commit_data(
    data_file_df,
    data_method_df,
    experiment_changes_out_file,
    experiment_files_out_file,
    experiment_methods_out_file,
    experiment_data,
):
    experiment_files_df = data_file_df[data_file_df.id.isin(experiment_data["id"])]
    experiment_files_df = pd.merge(experiment_files_df, pd.DataFrame(experiment_data), how="left", on=["id"])
    experiment_files_df = experiment_files_df.rename(columns={"id": "commit_id"})
    experiment_methods_df = data_method_df[data_method_df.id.isin(experiment_data["id"])]
    experiment_methods_df = pd.merge(experiment_methods_df, pd.DataFrame(experiment_data), how="left", on=["id"])
    experiment_methods_df = experiment_methods_df.rename(columns={"id": "commit_id"})

    experiment_methods_df = experiment_methods_df[
        [
            "commit_id",
            "author_email",
            "author_date",
            "additions",
            "deletions",
            "bug_count",
            "author_prior_changes",
            "author_prior_bugs",
            "file",
            "file_complexity",
            "file_prior_changes",
            "file_prior_bugs",
            "method",
            "method_complexity",
            "method_start_line",
            "method_end_line",
            "method_prior_changes",
            "method_prior_bugs",
            "practice",
        ]
    ]
    experiment_methods_df.to_csv(experiment_methods_out_file, index=False)
    experiment_files_df = experiment_files_df[
        [
            "commit_id",
            "author_email",
            "author_date",
            "additions",
            "deletions",
            "bug_count",
            "author_prior_changes",
            "author_prior_bugs",
            "file",
            "file_complexity",
            "file_prior_changes",
            "file_prior_bugs",
        ]
    ]
    experiment_files_df = experiment_files_df.drop_duplicates()
    experiment_files_df = experiment_files_df.reset_index(drop=True)
    experiment_files_df.to_csv(experiment_files_out_file, index=False)

    experiment_changes_df = pd.merge(
        experiment_files_df,
        experiment_methods_df[["commit_id", "method_risk_score_relative"]].drop_duplicates(),
        how="left",
        on="commit_id",
    )
    experiment_changes_df["risk_score"] = (
        experiment_changes_df["author_risk_score"]
        + experiment_changes_df["file_risk_score_relative"]
        + experiment_changes_df["method_risk_score_relative"]
    )
    experiment_changes_df["bug_density"] = experiment_changes_df["bug_count"] / (
        experiment_changes_df["additions"] + experiment_changes_df["deletions"]
    )

    experiment_changes_df = experiment_changes_df.sort_values(by="risk_score", ascending=False)
    experiment_changes_df = experiment_changes_df[
        [
            "commit_id",
            "author_email",
            "author_date",
            "additions",
            "deletions",
            "bug_count",
            "author_prior_bugs",
            "author_risk_score",
            "file_risk_score_relative",
            "method_risk_score_relative",
            "practice",
            "risk_score",
            "bug_density",
            "author_prior_change_score",
            "author_recent_change_score",
            "author_file_awareness",
            "author_file_prior_changes",
            "author_prior_changes",
            "author_recent_changes",
        ]
    ]
    experiment_changes_df = experiment_changes_df.drop_duplicates()

    experiment_changes_df.to_csv(experiment_changes_out_file, index=False)
