Kedro Geospatial Template
--------------------------

This template is a starting point for creating a
[Kedro](https://docs.kedro.org/en/stable/index.html) project for
**Geospatial Data Sciences**. It is based on the standard Kedro template which
can be created using the command ``kedro new``.


The template contains the following features:

- **Kedro** project configuration.
- **Dockerfile** to run your project in a container.
    - Base image is set to [osgeo/gdal:ubuntu-small-latest](https://github.com/OSGeo/gdal/pkgs/container/gdal).
    - Dependencies defined in [requirements.txt](/src/requirements.txt) and [pyproject.toml](src/pyproject.toml)installed in the container using pip.
    - [template_devcontainer.json](/.devcontainer/template_devcontainer.json) to set up the configuration of the development container in VS Code, including volume mounting and vscode extensions.
- **Geospatial Dependencies** such as gdal, geopandas, eartengine-api, geemap, leafmap and duckdb are included in the [requirements.txt](/src/requirements.txt>).
- **Sphinx documentation** template to document your project.
- **Pre-commit configuration** to run pre-commit hooks for black, isort, and ruff defined in [pre-commit-config.yaml](pre-commit-config.yaml) and [pyproject.toml](/pyproject.toml).
- **Make file** to run linting and cleaning commands


GitHub Pages
------------
Documentation is hosted on GitHub Pages: [Documentation | Kedro Geospatial Template](https://ac-willeke.github.io/kedro-geospatial/html/index.html)
