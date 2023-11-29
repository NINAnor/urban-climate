.. Kedro Geospatial documentation master file, created by
   sphinx-quickstart on Wed Nov 29 12:03:34 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Documentation for the Kedro Geospatial Template
====================================================

This template is a starting point for creating a
`Kedro <https://docs.kedro.org/en/stable/index.html>`_ project for
**Geospatial Data Sciences**. It is based on the standard Kedro template which
can be created using the command ``kedro new``.

The template contains the following features:

- **Kedro** project configuration.
- **Dockerfile** to run your project in a container.
    - Base image is set to `osgeo/gdal:ubuntu-small-latest <https://github.com/OSGeo/gdal/pkgs/container/gdal>`_.
    - Dependencies defined in `requirements.txt </src/requirements.txt>`_ and `pyproject.toml <src/pyproject.toml>`_ installed in the container using pip.
    - `template_devcontainer.json </.devcontainer/template_devcontainer.json>`_ to set up the configuration of the development container in VS Code, including volume mounting and vscode extensions.
- **Geospatial Dependencies** such as gdal, geopandas, eartengine-api, geemap, leafmap and duckdb are included in the `requirements.txt </src/requirements.txt>`_.
- **Sphinx documentation** template to document your project.
- **Pre-commit configuration** to run pre-commit hooks for black, isort, and ruff defined in `pre-commit-config.yaml </pre-commit-config.yaml>`_ and `pyproject.toml </pyproject.toml>`_.
- **Make file** to run linting and cleaning commands

Table of Contents
=================
.. toctree::
   :maxdepth: 2

   env_setup
   project_setup
   kedro_readme
   modules


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
