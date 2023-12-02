Project Set Up
==============

**outside container** (git not installed in container)
1. Set up linting and pre-commit hooks:
    - `make codestyle` to run black, isort and ruff.
    - `make pre-commit` to run pre-commit on all files.

2. Run pre-commit hooks and commit your changes:
    - `git add .` to add all files to the staging area.
    - `make pre-commit` to run pre-commit on all files.
    - `git commit -m "commit message"` to commit your changes.


**How to use pre-commit:**

In case you executed `pre-commit install`, `pre-commit` hooks will be executed each time you will try to commit (`git commit`). If any of the checks fail or if any files that is going to be committed is changed (because a tool refactored or cleaned it), the commit will fail.

The suggested method to use `pre-commit` is to run it before trying to commit your changes, using `pre-commit run -a`. You can run this command multiple times, to check if the changes are ready to be committed.
After all the tests succeeded, the changes can be staged (`git add`) and committed.

**Note**: Solely install pre-commit in the standard distribution, not in the miniconda distribution or project-specific poetry environment.

**inside container**

3. Set up the Kedro configuration files:
    - check your data catalog: `conf/base/catalog.yaml`
    - set your credentials: `conf/local/template_credentials.yaml`
    *--> rename to credentials.yaml, do not commit to git!!*
    - check your logging configuration: `conf/base/logging.yaml``

4. Run the tests:

   ```bash
    # test kedro installation
    kedro info
    # run kedro tests from project root
    pytest
    ```
5. Run and visualize the Kedro Pipeline
   ```bash
    kedro run
    kedro viz
    ```
    This will output the error message below and open an empty kedro viz instance.
    ValueError: Pipeline contains no nodes after applying all provided filters
