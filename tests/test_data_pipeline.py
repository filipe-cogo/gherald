import csv
import json

import pytest
from git.repo.base import Repo

from gherald.data_pipeline import (
    extract_bugs_from_jira,
    extract_commit_code_changes,
    extract_commits_from_vcs,
    extract_defect_inducing_commits,
    extract_modified_files_commit,
    merge_its_vcs_data,
)

APACHE_COMMONS_REPO_URL = "https://github.com/apache/commons-lang"


@pytest.fixture
def apache_commons_lang():
    return "LANG"


@pytest.fixture
def dec_22():
    return "2022-01-01"


@pytest.fixture(scope="session")
def bugs_output_file(tmp_path_factory):
    return tmp_path_factory.mktemp("bugs") / "apache_commons_lang_bugs.json"


@pytest.fixture(scope="session")
def commits_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_commits.csv"
    return tmp_path_factory.mktemp("commits") / file_name


@pytest.fixture(scope="session")
def apache_commons_lang_repo(tmp_path_factory):
    repo_dir = tmp_path_factory.mktemp("commons-lang")

    Repo.clone_from(APACHE_COMMONS_REPO_URL, repo_dir)

    return repo_dir


@pytest.fixture(scope="session")
def buggy_commits_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_buggy_commits.csv"
    buggy_commits_file = tmp_path_factory.mktemp("buggy_commits") / file_name

    return buggy_commits_file


@pytest.fixture(scope="session")
def bug_inducing_commits_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_bug_inducing_commit_ids.csv"
    buggy_commits_file = tmp_path_factory.mktemp("bug_inducing_commits") / file_name

    return buggy_commits_file


@pytest.fixture(scope="session")
def modified_files_commit_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_commits_modified_files.csv"
    buggy_commits_file = tmp_path_factory.mktemp("modified_files_commits") / file_name

    return buggy_commits_file


@pytest.fixture(scope="session")
def commit_code_changes_information_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_code_changes.csv"
    buggy_commits_file = tmp_path_factory.mktemp("code_changes") / file_name

    return buggy_commits_file


def test_fetch_bugs(apache_commons_lang, dec_22, bugs_output_file):
    extract_bugs_from_jira(bugs_output_file, apache_commons_lang, dec_22)
    assert open(bugs_output_file) is not None


def test_saved_file_with_bugs(bugs_output_file):
    with open(bugs_output_file) as f:
        bugs = json.load(f)
        assert len(bugs) > 0


def test_fetch_commits(commits_output_file, apache_commons_lang, apache_commons_lang_repo):
    extract_commits_from_vcs(commits_output_file, apache_commons_lang, str(apache_commons_lang_repo), "master")

    with open(commits_output_file, newline="") as f:
        commits = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(commits)) > 0


def test_merge_bugs_commits(buggy_commits_output_file, bugs_output_file, commits_output_file):
    merge_its_vcs_data(buggy_commits_output_file, bugs_output_file, commits_output_file)

    with open(buggy_commits_output_file, newline="") as f:
        buggy_commits = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(buggy_commits)) > 0


def test_szz(bug_inducing_commits_output_file, apache_commons_lang_repo, buggy_commits_output_file):
    extract_defect_inducing_commits(
        bug_inducing_commits_output_file, apache_commons_lang_repo, buggy_commits_output_file
    )

    with open(bug_inducing_commits_output_file, newline="") as f:
        bug_inducing_commits = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(bug_inducing_commits)) > 0


def test_find_files_modified_in_commits(modified_files_commit_output_file, commits_output_file):
    extract_modified_files_commit(modified_files_commit_output_file, commits_output_file)

    with open(modified_files_commit_output_file, newline="") as f:
        modified_files_commits = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(modified_files_commits)) > 0


def test_extract_commit_code_changes_information(commit_code_changes_information_output_file, apache_commons_lang_repo):
    extract_commit_code_changes(commit_code_changes_information_output_file, str(apache_commons_lang_repo))

    with open(commit_code_changes_information_output_file, newline="") as f:
        code_changes_commits = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(code_changes_commits)) > 0


# TODO: write tests for `data_pipeline.data_filtering()`
