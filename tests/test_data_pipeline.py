import csv
import json

import pytest
from git.repo.base import Repo

from gherald.data_pipeline import (
    data_filtering,
    extract_bugs_from_jira,
    extract_commit_code_changes,
    extract_commits_from_vcs,
    extract_defect_inducing_commits,
    extract_modified_files_commit,
    merge_its_vcs_data,
)
from gherald.risk_assessment import (
    prepare_experiment_commit_data,
    preprocess_data,
    read_all_commits,
    read_commit_code,
    read_filtered_commits,
    risk_assessment,
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


@pytest.fixture(scope="session")
def filtered_commits_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_commits_filtered.csv"
    filtered_commits_file = tmp_path_factory.mktemp("filtered_commits") / file_name

    return filtered_commits_file


@pytest.fixture(scope="session")
def filtered_fixing_commits_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_bug_fixing_commit_ids_filtered.csv"
    filtered_fixing_commits_file = tmp_path_factory.mktemp("filtered_fixing_commits") / file_name

    return filtered_fixing_commits_file


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


def test_data_filtering(
    filtered_commits_output_file,
    filtered_fixing_commits_output_file,
    commits_output_file,
    bug_inducing_commits_output_file,
    commit_code_changes_information_output_file,
):

    data_filtering(
        filtered_commits_output_file,
        filtered_fixing_commits_output_file,
        commits_output_file,
        bug_inducing_commits_output_file,
        commit_code_changes_information_output_file,
    )

    with open(filtered_commits_output_file, newline="") as f:
        filtered_commits = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(filtered_commits)) > 0


@pytest.fixture(scope="session")
def file_risk_data_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_file_risk_data.csv"
    risk_data_file = tmp_path_factory.mktemp("risk") / file_name

    return risk_data_file


@pytest.fixture(scope="session")
def method_risk_data_output_file(tmp_path_factory):
    file_name = "apache_commons_lang_method_risk_data.csv"
    risk_data_file = tmp_path_factory.mktemp("risk") / file_name

    return risk_data_file


@pytest.fixture(scope="session")
def experiment_changes_out_file(tmp_path_factory):
    file_name = "apache_commons_lang_experiment_changes.csv"
    risk_data_file = tmp_path_factory.mktemp("risk") / file_name

    return risk_data_file


@pytest.fixture(scope="session")
def experiment_files_out_file(tmp_path_factory):
    file_name = "apache_commons_lang_experiment_files.csv"
    risk_data_file = tmp_path_factory.mktemp("risk") / file_name

    return risk_data_file


@pytest.fixture(scope="session")
def experiment_methods_out_file(tmp_path_factory):
    file_name = "apache_commons_lang_experiment_methods.csv"
    risk_data_file = tmp_path_factory.mktemp("risk") / file_name

    return risk_data_file


@pytest.fixture
def experiment_data():
    experiment_commits = [
        "06aea7e74cfe4a1578cb76672f1562132090c205",
        "9397608dd35a335d5e14813c0923f9419782980a",
        "d844d1eb5e5b530a82b77302f1f284fd2f924be3",
        "c1f9320476ab9e5f262fdf8a5b3e1ff70199aed8",
        "5acf310d08b2bc5182cf936616ef70938cb2c499",
        "16774d1c0d6d8aa4579f7c96b3fdb78bd118e5aa",
        "188933345a7ebad94f74ba0fb6e8bc6eb99552a6",
        "65392be352be6ccc8acf24405d819f60cd0d1a22",
        "4f85c164a1a4eeb8813b61cf46132fb91971b323",
        "eaa9269ac80c2a957cabed0c46173149a4137c24",
    ]
    practice = [0, 0, 0, 0, 0, 1, 1, 1, 0, 0]
    bug_count = [1.0, 1.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    experiment_data = {"id": experiment_commits, "practice": practice, "bug_count": bug_count}
    return experiment_data


def test_read_all_commits(commits_output_file):
    commits_df = read_all_commits(commits_output_file)
    assert not commits_df.empty


def test_read_filtered_commits(filtered_commits_output_file):
    commits_filtered_df = read_filtered_commits(filtered_commits_output_file)
    assert not commits_filtered_df.empty


def test_read_commit_code(commit_code_changes_information_output_file):
    code_df = read_commit_code(commit_code_changes_information_output_file)
    assert not code_df.empty


def test_preprocess_data(
    commits_output_file,
    filtered_commits_output_file,
    commit_code_changes_information_output_file,
    bug_inducing_commits_output_file,
    modified_files_commit_output_file,
):
    (
        bug_inducing_files,
        bug_inducing_methods,
        commits_df_files_expanded,
        commits_df_methods_expanded,
        data_df,
    ) = preprocess_data(
        commits_output_file,
        filtered_commits_output_file,
        commit_code_changes_information_output_file,
        bug_inducing_commits_output_file,
        modified_files_commit_output_file,
    )
    assert not bug_inducing_files.empty
    assert not bug_inducing_methods.empty
    assert not commits_df_files_expanded.empty
    assert not commits_df_methods_expanded.empty
    assert not data_df.empty


def test_risk_assessment(
    commits_output_file,
    filtered_commits_output_file,
    commit_code_changes_information_output_file,
    bug_inducing_commits_output_file,
    modified_files_commit_output_file,
    file_risk_data_output_file,
    method_risk_data_output_file,
):
    risk_assessment(
        commits_output_file,
        filtered_commits_output_file,
        commit_code_changes_information_output_file,
        bug_inducing_commits_output_file,
        modified_files_commit_output_file,
        file_risk_data_output_file,
        method_risk_data_output_file,
    )

    with open(file_risk_data_output_file, newline="") as f:
        risk_data = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(risk_data)) > 0

    with open(method_risk_data_output_file, newline="") as f:
        risk_data = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(risk_data)) > 0


def test_prepare_experiment_commit_data(
    file_risk_data_output_file,
    method_risk_data_output_file,
    experiment_changes_out_file,
    experiment_files_out_file,
    experiment_methods_out_file,
    experiment_data,
):
    prepare_experiment_commit_data(
        file_risk_data_output_file,
        method_risk_data_output_file,
        experiment_changes_out_file,
        experiment_files_out_file,
        experiment_methods_out_file,
        experiment_data,
    )

    with open(experiment_changes_out_file, newline="") as f:
        risk_data = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(risk_data)) > 0

    with open(experiment_files_out_file, newline="") as f:
        risk_data = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(risk_data)) > 0

    with open(experiment_methods_out_file, newline="") as f:
        risk_data = csv.reader(f, quoting=csv.QUOTE_NONE)

        assert len(list(risk_data)) > 0
