# gherald

<div align="center">

[![Build status](https://github.com/filipe-cogo/gherald/workflows/build/badge.svg?branch=master&event=push)](https://github.com/filipe-cogo/gherald/actions?query=workflow%3Abuild)
[![Python Version](https://img.shields.io/pypi/pyversions/gherald.svg)](https://pypi.org/project/gherald/)
[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/filipe-cogo/gherald/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/filipe-cogo/gherald/blob/master/.pre-commit-config.yaml)
[![Semantic Versions](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg)](https://github.com/filipe-cogo/gherald/releases)
[![License](https://img.shields.io/github/license/filipe-cogo/gherald)](https://github.com/filipe-cogo/gherald/blob/master/LICENSE)
![Coverage Report](assets/images/coverage.svg)

A python package to assess the risk of a change during code review.

</div>

## Installation

```bash
pip install -U gherald
```

or install with `Poetry`

```bash
poetry add gherald
```

## Usage 

### Run data pipeline

Below, we show an example of code to run the complete data pipeline, which includes the extraction of bug-fixing and bug-inducing commits from the Apache Commons-lang repo:

```python
from git.repo.base import Repo

# extract bug-fixing and bug-inducing commits from Apache commons-lang
def run_data_pipeline(
    bugs_output_file,
    commits_output_file,
    buggy_commits_output_file,
    bug_inducing_commits_output_file,
    modified_files_commit_output_file,
    commit_code_changes_information_output_file,
    filtered_commits_output_file,
    filtered_fixing_commits_output_file
):
    apache_commons_lang_suffix = "LANG"
    cloned_repo_path = "path/to/cloned/repo"
    cutoff_date = "2022-01-01"
    branch_name = "master"

    # clone Apache commons-lang 
    Repo.clone_from("https://github.com/apache/commons-lang", cloned_repo_path)

    extract_bugs_from_jira(bugs_output_file, apache_commons_lang_suffix, cutoff_date)
    extract_commits_from_vcs(
        commits_output_file, apache_commons_lang_suffix, cloned_repo_path, branch_name)
    merge_its_vcs_data(buggy_commits_output_file, bugs_output_file, commits_output_file)
    extract_defect_inducing_commits(
        bug_inducing_commits_output_file, cloned_repo_path, buggy_commits_output_file)
    extract_modified_files_commit(modified_files_commit_output_file, commits_output_file)
    extract_commit_code_changes(commit_code_changes_information_output_file, cloned_repo_path)
    data_filtering(
        filtered_commits_output_file,
        filtered_fixing_commits_output_file,
        commits_output_file,
        bug_inducing_commits_output_file,
        commit_code_changes_information_output_file,
    )

    return


run_data_pipeline(
    "apache_commons_lang_bugs.json",
    "apache_commons_lang_commits.csv",
    "apache_commons_lang_buggy_commits.csv",
    "apache_commons_lang_bug_inducing_commit_ids.csv",
    "apache_commons_lang_commits_modified_files.csv",
    "apache_commons_lang_code_changes.csv",
    "apache_commons_lang_commits_filtered.csv",
    "apache_commons_lang_bug_fixing_commit_ids_filtered.csv"
)
```

The `run_data_pipeline` is a self-written function that calls the `extract_bugs_from_jira`, `extract_commits_from_vcs`, `merge_its_vcs_data`, `extract_defect_inducing_commits`, `extract_modified_files_commit`, and `extract_commit_code_changes` functions in order. Below, we explain each of the parameters of `run_data_pipeline`:

- `bugs_output_file`: The output file to which bug information will be written.
- `commits_output_file`: The output file to which commit information will be written.
- `buggy_commits_output_file`: The output file to which bug-fixing commits information will be written.
- `bug_inducing_commits_output_file`: The output file to which bug-inducing commits information will be written.
- `modified_files_commit_output_file`:  The output file to which filtered commit information will be written.
- `commit_code_changes_information_output_file`: The output file to which code change information will be written.
- `filtered_commits_output_file`: The output file to which filtered bug-inducing commits information will be written.
- `filtered_fixing_commits_output_file`: The output file to which filtered bug-fixing commits information will be written.

  Inside `run_data_pipeline`, we define four constants, `apache_commons_lang_suffix`, `cloned_repo_path`, `cutoff_date`, `branch_name`, meaning, respectively, the suffix used by the repository in the Jira’s URL from which commit information is extracted (Apache Common-Lang in our example), the local path to the cloned Jira’s repository, the most recent date for which commit information is crawled from the Jira’s repository, and the branch from which commit information is extracted. Finally, `Repo.clone_from` clones the repository in the specified folder.  

### Run risk assessment

The `risk_assessment` is a self_written function that assesses the risk of each change after the filtering step.
We use the output files generated by `run_data_pipeline` as parameters:

- `commits_file`: The file generated by the commits extraction step.
- `filtered_commits_file`: The file generated by the commit filtering step.
- `commit_code_changes_file`: The file generated by the step that extracts per-commit code changes information.
- `bug_inducing_commit_file`: The file generated by the step that extracts bug inducing commits.
- `commit_modified_files`: The file generated by the step that extracts modified files information.
Additionally, we store the risk assessment information for each file and method into file_risk_data_output_file and method_risk_data_output_file, respectively.

The `prepare_experiment_commit_data` is a self-written functions that computes the risk assessment information for the selected experimental changes.
Below, we explain each of the parameters of `prepare_experiment_commit_data`:

- `file_risk_data_output_file`: The file generated by the risk assessment step, storing the risk assessment information for each file.
- `method_risk_data_output_file`: The file generated by the risk assessment step, storing the risk assessment information for each method.
- `experiment_changes_out_file`: The output file to which the risk assessment information for the experimental changes will be written.
- `experiment_files_out_file`: The output file to which the risk assessment information for the files in each experimental changes will be written.
- `experiment_methods_out_file`: The output file to which the risk assessment information for the methods in each experimental changes will be written.
- `experiment_data`: The experimental changes data stored in a dictionary, containing the experimental change IDs, the number of defects in each experiment change, etc.

Below, we show an example of code to run the risk_assessment and prepare_experiment_commit_data, which includes the risk assessment for changes in the Apache Commons-lang repo and experimental changes selection process:

```python
risk_assessment(
    "apache_commons_lang_commits.csv",
    "apache_commons_lang_commits_filtered.csv",
    "apache_commons_lang_code_changes.csv",
    "apache_commons_lang_bug_inducing_commit_ids.csv",
    "apache_commons_lang_commits_modified_files.csv",
    "apache_commons_lang_file_risk_data.csv",
    "apache_commons_lang_method_risk_data.csv"
)
experiment_data = {
    "id": [
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
    ],
    "practice": [0, 0, 0, 0, 0, 1, 1, 1, 0, 0],
    "bug_count": [1.0, 1.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
  }
prepare_experiment_commit_data(
    "apache_commons_lang_file_risk_data.csv",
    "apache_commons_lang_method_risk_data.csv",
    "apache_commons_lang_experiment_changes.csv",
    "apache_commons_lang_experiment_files.csv",
    "apache_commons_lang_experiment_methods.csv",
    experiment_data
)
```

## Development

### Initialize code

1. Initialize `git` inside the repo:

```bash
cd gherald && git init
```

2. If you don't have `Poetry` installed run:

```bash
make poetry-download
```

3. Initialize poetry and install `pre-commit` hooks:

```bash
make install
make pre-commit-install
```

4. Run the codestyle:

```bash
make codestyle
```

5. Upload initial code to GitHub:

```bash
git add .
git commit -m ":tada: Initial commit"
git branch -M main
git remote add origin https://github.com/filipe-cogo/gherald.git
git push -u origin main
```

### Poetry

Check [its documentation](https://python-poetry.org/docs/).

<details>
<summary>Details about Poetry</summary>
<p>

Poetry's [commands](https://python-poetry.org/docs/cli/#commands) are very intuitive and easy to learn, like:

- `poetry add numpy@latest`
- `poetry run pytest`
- `poetry publish --build`

etc
</p>
</details>

### Building and releasing

Building a new version of the package:

- Bump the version of the package `poetry version <version>`. You can pass the new version explicitly, or a rule such as `major`, `minor`, or `patch`. For more details, refer to the [Semantic Versions](https://semver.org/) standard.
- Make a commit to `GitHub`.
- Create a `GitHub release`.
- And... publish 🙂 `poetry publish --build`

### Makefile usage

[`Makefile`](https://github.com/filipe-cogo/gherald/blob/master/Makefile) contains a lot of functions for faster development.

<details>
<summary>1. Download and remove Poetry</summary>
<p>

To download and install Poetry run:

```bash
make poetry-download
```

To uninstall

```bash
make poetry-remove
```

</p>
</details>

<details>
<summary>2. Install all dependencies and pre-commit hooks</summary>
<p>

Install requirements:

```bash
make install
```

Pre-commit hooks can be installed after `git init` via

```bash
make pre-commit-install
```

</p>
</details>

<details>
<summary>3. Codestyle</summary>
<p>

Automatic formatting uses `pyupgrade`, `isort` and `black`.

```bash
make codestyle

# or use synonym
make formatting
```

Codestyle checks only, without rewriting files:

```bash
make check-codestyle
```

> Note: `check-codestyle` uses `isort`, `black` and `darglint` library

Update all dev libraries to the latest version using one comand

```bash
make update-dev-deps
```

<details>
<summary>4. Code security</summary>
<p>

```bash
make check-safety
```

This command launches `Poetry` integrity checks as well as identifies security issues with `Safety` and `Bandit`.

```bash
make check-safety
```

</p>
</details>

</p>
</details>

<details>
<summary>5. Type checks</summary>
<p>

Run `mypy` static type checker

```bash
make mypy
```

</p>
</details>

<details>
<summary>6. Tests with coverage badges</summary>
<p>

Run `pytest`

```bash
make test
```

</p>
</details>

<details>
<summary>7. All linters</summary>
<p>

There is a command to run all linters in one:

```bash
make lint
```

the same as:

```bash
make test && make check-codestyle && make mypy && make check-safety
```

</p>
</details>

<details>
<summary>8. Docker</summary>
<p>

```bash
make docker-build
```

which is equivalent to:

```bash
make docker-build VERSION=latest
```

Remove docker image with

```bash
make docker-remove
```

More information [about docker](https://github.com/filipe-cogo/gherald/tree/master/docker).

</p>
</details>

<details>
<summary>9. Cleanup</summary>
<p>
Delete pycache files

```bash
make pycache-remove
```

Remove package build

```bash
make build-remove
```

Delete .DS_STORE files

```bash
make dsstore-remove
```

Remove .mypycache

```bash
make mypycache-remove
```

Or to remove all above run:

```bash
make cleanup
```

</p>
</details>

## 📈 Releases

You can see the list of available releases on the [GitHub Releases](https://github.com/filipe-cogo/gherald/releases) page.

We follow [Semantic Versions](https://semver.org/) specification.

We use [`Release Drafter`](https://github.com/marketplace/actions/release-drafter). As pull requests are merged, a draft release is kept up-to-date listing the changes, ready to publish when you’re ready. With the categories option, you can categorize pull requests in release notes using labels.

### List of labels and corresponding titles

|               **Label**               |  **Title in Releases**  |
| :-----------------------------------: | :---------------------: |
|       `enhancement`, `feature`        |       🚀 Features       |
| `bug`, `refactoring`, `bugfix`, `fix` | 🔧 Fixes & Refactoring  |
|       `build`, `ci`, `testing`        | 📦 Build System & CI/CD |
|              `breaking`               |   💥 Breaking Changes   |
|            `documentation`            |    📝 Documentation     |
|            `dependencies`             | ⬆️ Dependencies updates |

You can update it in [`release-drafter.yml`](https://github.com/filipe-cogo/gherald/blob/master/.github/release-drafter.yml).

GitHub creates the `bug`, `enhancement`, and `documentation` labels for you. Dependabot creates the `dependencies` label. Create the remaining labels on the Issues tab of your GitHub repository, when you need them.

## 🛡 License

[![License](https://img.shields.io/github/license/filipe-cogo/gherald)](https://github.com/filipe-cogo/gherald/blob/master/LICENSE)

This project is licensed under the terms of the `MIT` license. See [LICENSE](https://github.com/filipe-cogo/gherald/blob/master/LICENSE) for more details.

## 📃 Citation

```bibtex
@misc{gherald,
  author = {University of Waterloo},
  title = {A python package to assess the risk of a change during code review.},
  year = {2023},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/filipe-cogo/gherald}}
}
```

## Credits [![🚀 Your next Python package needs a bleeding-edge project structure.](https://img.shields.io/badge/python--package--template-%F0%9F%9A%80-brightgreen)](https://github.com/TezRomacH/python-package-template)

This project was generated with [`python-package-template`](https://github.com/TezRomacH/python-package-template)
