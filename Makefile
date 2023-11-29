.PHONY: codestyle formatting docstring pre-commit clean clean-venv clean-build clean-pyc clean-linting clean-test clean-log poetry-clean

.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV_NAME := .venv

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help:
	@python3 -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

# --------------------------------------------------------------------------- #
# Style targets
# --------------------------------------------------------------------------- #

codestyle: ## check codestyle (black, isort, ruff)
	isort --settings-path pyproject.toml ./
	black --config pyproject.toml ./
	ruff check ./

docstring: ## USE WITH CAUTION update docstring to google style (kedro standard)
	pyment -w -o google ./

pre-commit: ## run pre-commit on all files
	pre-commit run -a

# --------------------------------------------------------------------------- #
# Cleaning targets
# --------------------------------------------------------------------------- #

clean: clean-build clean-pyc clean-linting clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '.ipynb_checkpoints' -exec rm -fr {} +

clean-linting: ## remove linting artifacts
	find . -name '.ruff_cache' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -fr .pytest_cache
