import ast
import csv
import json
import math
import re
import subprocess
import sys
from datetime import timezone

import nltk
import numpy as np
import pandas as pd
import requests
from nltk.tokenize import word_tokenize
from pandarallel import pandarallel
from pydriller import Git, Repository

pandarallel.initialize()
nltk.download("punkt")

JIRA_API_URL = "https://issues.apache.org/jira/rest/api/2/search"


def extract_bugs_from_jira(
    out_file, project_name, creation_date_upper_bound, pagesize=1000, its_search_url=JIRA_API_URL
):

    headers = {"Content-type": "application/json", "Accept": "application/json"}

    body = {
        "expand": ["names", "schema"],
        "jql": f"project = {project_name} AND \
                issuetype = Bug AND \
                created < {creation_date_upper_bound}",
        "fields": ["created", "assignee"],
        "maxResults": pagesize,
        "startAt": 0,
    }

    f = open(out_file, "w")
    f.write("[")
    firstIssue = 1

    try:
        for _ in range(1):
            res = requests.post(its_search_url, headers=headers, json=body)
            if res.status_code == 200:
                res_obj = res.json()
                for issue in res_obj["issues"]:
                    if firstIssue == 1:
                        firstIssue = 0
                    else:
                        f.write(",")
                    # assignee = issue["fields"]["assignee"]
                    obj = {"key": issue["key"], "created": issue["fields"]["created"]}
                    f.write(json.dumps(obj))
                body["startAt"] += pagesize
            else:
                print(
                    "Unexpected response code \
                      from REST server: {}".format(
                        res.status_code
                    ),
                    file=sys.stderr,
                )

    except Exception as e:
        print(
            "Unexpected exception \
              from issue at {}: {}".format(
                body["startAt"], e
            ),
            file=sys.stderr,
        )

    f.write("]")


def extract_commits_from_vcs(out_file, project_name, repo_path, branch):
    with open(out_file, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "id",
                "author_name",
                "author_email",
                "author_date",
                "committer_name",
                "committer_email",
                "commit_date",
                "files",
                "additions",
                "deletions",
                "issues",
                "msg",
            ]
        )
        for commit in Repository(repo_path, only_in_branch=f"{branch}", order="date-order").traverse_commits():
            files = []
            for file in commit.modified_files:
                filename = file.new_path
                filename_old = None
                if file.change_type.name == "DELETE":
                    filename = file.old_path
                if file.change_type.name == "RENAME":
                    filename_old = file.old_path
                methods = []
                for m in file.changed_methods:
                    methods.append((m.name, m.long_name, m.start_line, m.end_line, m.complexity))
                files.append(
                    {
                        "filename": filename,
                        "filename_old": filename_old,
                        "file_complexity": file.complexity,
                        "methods": methods,
                    }
                )
            msg = commit.msg
            issues = []
            if f"{project_name}-" in msg:
                for text in re.findall(rf"{project_name}-\d*", msg):
                    issues.append(text)

            writer.writerow(
                [
                    commit.hash,
                    commit.author.name,
                    commit.author.email,
                    commit.author_date.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    commit.committer.name,
                    commit.committer.email,
                    commit.committer_date.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    files,
                    commit.insertions,
                    commit.deletions,
                    issues,
                    msg,
                ]
            )


def merge_its_vcs_data(out_file, bugs_file_path, commits_file_path, strata_per_year=4):
    # collect commits information
    commits_df = pd.read_csv(commits_file_path)
    commits_df.issues = commits_df.issues.apply(ast.literal_eval)
    commits_df["author_date_ts"] = commits_df["author_date"].apply(lambda x: int(pd.Timestamp(x).timestamp()))
    min_author_date = commits_df["author_date_ts"].min()
    strata_size = 60 * 60 * 24 * (365.25 / strata_per_year)
    commits_df["author_date_strata"] = commits_df["author_date_ts"].apply(
        lambda x: int((x - min_author_date) / strata_size)
    )
    commits_df = commits_df.sort_values(by="author_date")

    # collect bugs information
    issues_df = pd.read_json(bugs_file_path)
    issues_df.columns = ["issue", "issue_created"]
    issues_df["issue_created"] = issues_df["issue_created"].str[:-9]
    issues_df["issue_created"] = pd.to_datetime(issues_df["issue_created"], format="%Y-%m-%dT%H:%M:%S")

    # reshape commits dataframe
    temp = commits_df.explode("issues")
    temp = temp.dropna(subset=["issues"])
    temp = temp.rename(columns={"issues": "issue"})

    # merge information
    bug_fixing_commits_df = pd.merge(temp, issues_df, how="inner", on="issue")
    bug_fixing_commit_ids = bug_fixing_commits_df[["id", "issue_created"]]
    bug_fixing_commit_ids.drop_duplicates(subset=["id"])
    bug_fixing_commit_ids.to_csv(out_file, index=False)


def extract_defect_inducing_commits(out_file, repo_path, buggy_commits_file):
    def szz(commit_id):
        commit = gr.get_commit(commit_id)
        return gr.get_commits_last_modified_lines(commit)

    gr = Git(repo_path)

    bug_inducing_commit_ids = pd.read_csv(buggy_commits_file)
    bug_inducing_commit_ids = bug_inducing_commit_ids.rename(columns={"id": "bug_fixing_commit_id"})
    bug_inducing_commit_ids = bug_inducing_commit_ids.rename(columns={"issue_created": "bug_fixing_issue_created_date"})
    bug_inducing_commit_ids["id"] = bug_inducing_commit_ids["bug_fixing_commit_id"].apply(lambda x: szz(x))

    bug_inducing_commit_ids.to_csv(out_file, index=False)


def extract_modified_files_commit(out_file, commits_file):
    commit_files = pd.read_csv(commits_file)
    commit_files["files"] = commit_files["files"].apply(lambda x: ast.literal_eval(str(x)))
    commit_files = commit_files.explode("files")
    commit_files = commit_files.dropna(subset=["files"])
    commit_files["file"] = commit_files["files"].apply(lambda x: x["filename"])
    commit_files["file_newpath"] = commit_files["file"]
    commit_files["file_oldpath"] = commit_files["files"].apply(lambda x: x["filename_old"])
    commit_files["file_complexity"] = commit_files["files"].apply(lambda x: x["file_complexity"])
    commit_files["methods"] = commit_files["files"].apply(lambda x: x["methods"])

    commit_files = commit_files[
        ["id", "author_date", "file", "file_newpath", "file_oldpath", "file_complexity", "methods"]
    ]
    commit_files = commit_files.fillna(value=np.nan)
    commit_files = commit_files.reset_index(drop=True)

    commit_files_renamed = commit_files[commit_files["file_oldpath"].notnull()]
    commit_files_renamed = commit_files_renamed.sort_values(by="author_date", ascending=False)

    commit_files["file_oldpaths"] = [list() for x in range(len(commit_files.index))]
    for index, row in commit_files_renamed.iterrows():
        oldpath = row["file_oldpath"]
        newpath = row["file_newpath"]
        commit_files.loc[commit_files["file_newpath"] == newpath, "file_oldpaths"].apply(lambda x: x.append(oldpath))
        commit_files.loc[commit_files["file_newpath"] == newpath, "file_newpath"] = oldpath

    commit_files = commit_files[["id", "author_date", "file", "file_complexity", "methods", "file_oldpaths"]]
    commit_files.to_csv(out_file, index=False)


def extract_commit_code_changes(out_file, repo_path, branch="master"):
    def diff_file_index(code):
        file_index = [i for i, c in enumerate(code) if c.startswith("diff")]
        return file_index

    def get_line_numbers(line):
        token = line.split(" ")
        numbers_old_file1 = token[1]
        numbers_old_file2 = token[2]
        numbers_new_file = token[3]
        delete_line_number1 = int(numbers_old_file1.split(",")[0].replace("-", "")) - 1
        delete_line_number2 = int(numbers_old_file2.split(",")[0].replace("-", "")) - 1
        additions_line_number = int(numbers_new_file.split(",")[0]) - 1
        return delete_line_number1, delete_line_number2, additions_line_number

    def parse_diff_file(diff_file):
        modified_lines = {
            "added": [],
            "deleted": [],
        }

        count_deletions1 = 0
        count_deletions2 = 0
        count_additions = 0

        for line in diff_file:
            if line.startswith("@@@"):
                count_deletions1,
                count_deletions2,
                count_additions = get_line_numbers(line)
                continue

            if count_deletions1 > 0 and count_deletions2 > 0 and count_additions > 0:
                count_deletions1 += 1
                count_deletions2 += 1
                count_additions += 1

                if line.startswith("- "):
                    modified_lines["deleted"].append((count_deletions1, 0, line[2:]))
                    count_additions -= 1
                    count_deletions2 -= 1
                if line.startswith(" -"):
                    modified_lines["deleted"].append((0, count_deletions2, line[2:]))
                    count_additions -= 1
                    count_deletions1 -= 1
                if line.startswith("--"):
                    modified_lines["deleted"].append((count_deletions1, count_deletions2, line[2:]))
                    count_additions -= 1

                if line.startswith("+ "):
                    modified_lines["added"].append((count_additions, line[2:]))
                    count_deletions1 -= 1
                if line.startswith(" +"):
                    modified_lines["added"].append((count_additions, line[2:]))
                    count_deletions2 -= 1
                if line.startswith("++"):
                    modified_lines["added"].append((count_additions, line[2:]))
                    count_deletions1 -= 1
                    count_deletions2 -= 1

                if line == r"\ No newline at end of file":
                    count_deletions1 -= 1
                    count_deletions2 -= 1
                    count_additions -= 1

        return modified_lines

    def parse_diff(diff):
        parsed_diff = []
        lines = diff.split("\n")
        code = [line.rstrip() for line in lines]
        indexes = diff_file_index(code=code)
        for i in range(0, len(indexes)):
            if i == len(indexes) - 1:
                diff_file = code[indexes[i] :]
            else:
                diff_file = code[indexes[i] : indexes[i + 1]]

            file = diff_file[0].split(" ")[-1]
            parsed_diff_file = parse_diff_file(diff_file)
            parsed_diff.append({"filename": file, "parsed_diff_file": parsed_diff_file})

        return parsed_diff

    with open(out_file, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "code_changes"])
        for commit in Repository(
            repo_path, only_in_branch=f"{branch}", order="date-order", skip_whitespaces=True
        ).traverse_commits():
            code_changes = []
            if len(commit.parents) > 1:
                if len(commit.parents) == 2:
                    command = f"cd {repo_path}; \
                            git diff {commit.hash} {commit.hash}^@"
                    ret = subprocess.run(command, capture_output=True, shell=True)
                    try:
                        parsed_diff = parse_diff(ret.stdout.decode())
                        for file in parsed_diff:
                            code_changes.append(
                                {
                                    "filename": file["filename"],
                                    "added_code": file["parsed_diff_file"]["added"],
                                    "removed_code": file["parsed_diff_file"]["deleted"],
                                }
                            )
                    except Exception:
                        print(
                            "Failed to parse diff \
                              for merge commit {}".format(
                                commit.hash
                            )
                        )
                else:
                    print(
                        "Commit {} has more than \
                          two parents".format(
                            commit.hash
                        )
                    )
            else:
                for file in commit.modified_files:
                    filename = file.new_path
                    if file.change_type.name == "DELETE":
                        filename = file.old_path
                    code_changes.append(
                        {
                            "filename": filename,
                            "added_code": file.diff_parsed["added"],
                            "removed_code": file.diff_parsed["deleted"],
                        }
                    )

            writer.writerow([commit.hash, code_changes])


def data_filtering(
    out_file_commits, out_file_bug_fixing, commits_file, bug_inducing_file, commit_code_changes_file, strata_per_year=4
):
    def get_inducing_commits(obj):
        obj = ast.literal_eval(obj)
        bug_inducing_ids = set()
        for _, ids in obj.items():
            for value in ids:
                id = value
                if id not in bug_inducing_ids:
                    bug_inducing_ids.add(id)
        return list(bug_inducing_ids)

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
            added_code = filter_code_lines(f["added_code"])
            additions += len(added_code)
        return additions

    def get_deletions(x):
        deletions = 0
        for f in x:
            deleted_code = filter_code_lines(f["removed_code"])
            deletions += len(deleted_code)
        return deletions

    def get_filenames(x):
        files = []
        for f in x:
            files.append(f["filename"])
        return files

    commits_df = pd.read_csv(commits_file)
    commits_df["author_date_ts"] = commits_df["author_date"].parallel_apply(lambda x: int(pd.Timestamp(x).timestamp()))

    min_author_date = commits_df["author_date_ts"].min()
    strata_size = 60 * 60 * 24 * (365.25 / strata_per_year)
    commits_df["author_date_strata"] = commits_df["author_date_ts"].apply(
        lambda x: int((x - min_author_date) / strata_size)
    )
    commits_df = commits_df.sort_values(by="author_date")
    commits_df = commits_df[["id", "author_email", "author_date", "author_date_ts", "author_date_strata"]]

    bug_inducing_commits = pd.read_csv(bug_inducing_file)
    bug_inducing_commits["id"] = bug_inducing_commits["id"].apply(lambda x: get_inducing_commits(x))
    bug_inducing_commits = bug_inducing_commits.explode("id")
    bug_inducing_commits = bug_inducing_commits.dropna(subset=["id"])
    bug_inducing_commits = bug_inducing_commits.reset_index(drop=True)
    bug_inducing_commits["buggy"] = 1

    commits_df = pd.merge(commits_df, bug_inducing_commits, how="outer", on="id")
    commits_df["buggy"] = commits_df["buggy"].fillna(0)

    # F1
    code_df = pd.read_csv(commit_code_changes_file)
    code_df.code_changes = code_df.code_changes.apply(ast.literal_eval)

    code_df["additions"] = code_df["code_changes"].parallel_apply(lambda x: get_additions(x))
    code_df["deletions"] = code_df["code_changes"].parallel_apply(lambda x: get_deletions(x))

    code_df["files"] = code_df["code_changes"].parallel_apply(lambda x: get_filenames(x))
    code_df["num_of_files"] = code_df["files"].parallel_apply(lambda x: len(x))
    code_df = code_df.drop(columns=["code_changes", "files"])

    commits_df = pd.merge(commits_df, code_df, how="left", on="id")

    commits_filtered_df = commits_df[commits_df["additions"] + commits_df["deletions"] > 0]
    commits_filtered_df = commits_filtered_df.reset_index(drop=True)

    # F2
    commits_filtered_df["author_date"] = pd.to_datetime(commits_filtered_df["author_date"])
    commits_filtered_df["bug_fixing_issue_created_date"] = pd.to_datetime(
        commits_filtered_df["bug_fixing_issue_created_date"]
    )
    commits_filtered_df = commits_filtered_df[
        (commits_filtered_df["buggy"] == 0)
        | (commits_filtered_df["author_date"] < commits_filtered_df["bug_fixing_issue_created_date"])
    ]

    commits_filtered_df = commits_filtered_df.reset_index(drop=True)

    # F3a
    commits_filtered_df = commits_filtered_df[
        commits_filtered_df["additions"] + commits_filtered_df["deletions"] < 10000
    ]
    commits_filtered_df = commits_filtered_df.reset_index(drop=True)

    # F3b
    commits_filtered_df = commits_filtered_df[commits_filtered_df["num_of_files"] < 100]
    commits_filtered_df = commits_filtered_df.reset_index(drop=True)

    # F3c
    commits_filtered_df = commits_filtered_df[commits_filtered_df["additions"] > 0]
    commits_filtered_df = commits_filtered_df.reset_index(drop=True)

    commits_temp_df = commits_filtered_df.drop_duplicates(subset=["id"])

    # F4
    commits_filtered_df = commits_filtered_df[commits_filtered_df["author_date_strata"] < 75]
    commits_filtered_df = commits_filtered_df.reset_index(drop=True)

    commits_temp_df = commits_filtered_df[commits_filtered_df["bug_fixing_commit_id"].notnull()]
    F5_df = commits_temp_df.groupby("bug_fixing_commit_id").size().reset_index(name="fix_count")

    F5_median = F5_df["fix_count"].median()
    F5_mean = F5_df["fix_count"].mean()
    F5_df["diff"] = F5_df["fix_count"].apply(lambda x: abs(x - F5_mean))
    F5_mad = F5_df["diff"].mean()
    F5_df = F5_df.drop(columns=["diff"])
    F5_upper_mad = F5_median + F5_mad

    commits_temp_df = commits_filtered_df[commits_filtered_df["bug_fixing_commit_id"].notnull()]
    F6_df = commits_temp_df.groupby("id").size().reset_index(name="bug_count")

    F6_median = F6_df["bug_count"].median()
    F6_mean = F6_df["bug_count"].mean()
    F6_df["diff"] = F6_df["bug_count"].apply(lambda x: abs(x - F6_mean))
    F6_mad = F6_df["diff"].mean()
    F6_df = F6_df.drop(columns=["diff"])
    F6_upper_mad = F6_median + F6_mad

    # F5
    commits_filtered_df = pd.merge(commits_filtered_df, F5_df, how="left", on="bug_fixing_commit_id")
    commits_filtered_df = commits_filtered_df[
        (commits_filtered_df["buggy"] == 0) | (commits_filtered_df["fix_count"] <= math.ceil(F5_upper_mad))
    ]
    commits_filtered_df = commits_filtered_df.reset_index(drop=True)

    F5_df[F5_df["fix_count"] <= math.ceil(F5_upper_mad)].to_csv(out_file_bug_fixing, index=False)

    # F6
    commits_filtered_df = pd.merge(commits_filtered_df, F6_df, how="left", on="id")
    commits_filtered_df = commits_filtered_df[
        (commits_filtered_df["buggy"] == 0) | (commits_filtered_df["bug_count"] <= math.ceil(F6_upper_mad))
    ]
    commits_filtered_df = commits_filtered_df.reset_index(drop=True)
    commits_filtered_df = commits_filtered_df.drop(columns=["bug_count"])
    commits_temp_df = commits_filtered_df[commits_filtered_df["bug_fixing_commit_id"].notnull()]
    F6_df = commits_temp_df.groupby("id").size().reset_index(name="bug_count")
    commits_filtered_df = pd.merge(commits_filtered_df, F6_df, how="left", on="id")

    commits_filtered_df = commits_filtered_df.drop(columns=["fix_count", "bug_count"])
    commits_temp_df = commits_filtered_df[commits_filtered_df["bug_fixing_commit_id"].notnull()]
    F5_df = commits_temp_df.groupby("bug_fixing_commit_id").size().reset_index(name="fix_count")
    commits_filtered_df = pd.merge(commits_filtered_df, F5_df, how="left", on="bug_fixing_commit_id")
    F6_df = commits_temp_df.groupby("id").size().reset_index(name="bug_count")
    commits_filtered_df = pd.merge(commits_filtered_df, F6_df, how="left", on="id")

    commits_filtered_df["fix_count"] = commits_filtered_df["fix_count"].fillna(0)
    commits_filtered_df["bug_count"] = commits_filtered_df["bug_count"].fillna(0)

    commits_temp_df = commits_filtered_df.drop(
        columns=["bug_fixing_commit_id", "bug_fixing_issue_created_date", "fix_count"]
    )
    commits_temp_df = commits_temp_df.drop_duplicates(subset=["id"])
    commits_temp_df = commits_temp_df.reset_index(drop=True)

    # display(commits_temp_df)
    commits_temp_df.to_csv(out_file_commits, index=False)


def run_pipeline(
    bugs_output_file,
    commits_output_file,
    buggy_commits_output_file,
    bug_inducing_commits_output_file,
    modified_files_commit_output_file,
    commit_code_changes_information_output_file,
    filtered_fixing_commits_output_file,
    filtered_commits_output_file,
    jira_repo,
    jira_repo_clone_dir,
    jira_repo_branch,
    github_repo_url,
    until,
):
    """'
        Run the data pipeline.

        Args:
            bugs_output_file (str): The name of the file generated by \
                the bug extraction step.
            commits_output_file (str): The name of the file generated by \
                the commits extraction step.
            buggy_commits_output_file (str): The name of the file generated by \
                the buggy commits extraction step.
            bug_inducing_commits_output_file (str): The name of the file generated by \
                the step that extracts bug inducing commits.
            modified_files_commit_output_file (str): The name of the file generated by \
                the step that extracts modified files information.
            commit_code_changes_information_output_file (str): The name of the file generated by\
                  the step that extracts per-commit code changes information.
            filtered_commits_output_file (str): The name of the file generated by \
                the commit filtering step.
            filtered_fixing_commits_output_file (str): The name of the file generated by \
                the fixing commit filtering step.
            jira_repo (str): The prefix of the Jira's project.
            jira_repo_clone_dir (str): The directory where the Jira's repository is cloned.
            jira_repo_branch (str): The branch from where to pull the information from \
                the Jira repository.
            github_repo_url (str): The URL of the GitHub repository of the project from \
                which bug inducing commits are extracted.
            until (str): The data of the latest commit to be extracted.
    """
    extract_bugs_from_jira(bugs_output_file, jira_repo, until)
    extract_commits_from_vcs(commits_output_file, jira_repo, jira_repo_clone_dir, jira_repo_branch)
    merge_its_vcs_data(buggy_commits_output_file, bugs_output_file, commits_output_file)
    extract_defect_inducing_commits(bug_inducing_commits_output_file, github_repo_url, buggy_commits_output_file)
    extract_modified_files_commit(modified_files_commit_output_file, commits_output_file)
    extract_commit_code_changes(commit_code_changes_information_output_file, github_repo_url)
    data_filtering(
        filtered_commits_output_file,
        filtered_fixing_commits_output_file,
        commits_output_file,
        bug_inducing_commits_output_file,
        commit_code_changes_information_output_file,
    )
