Local Climate Regulation
========================

[![Documentation Status](https://readthedocs.org/projects/kedro-geospatial/badge/?version=latest)](https://ac-willeke.github.io/kedro-geospatial/html/index.html)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


This repository provides a workflow for analyzing the impact of urban trees on local climate regulation. This workflow is an adapted version of the methodolgy described in [Venter et al. (2020)](https://www.sciencedirect.com/science/article/pii/S0048969719361893).




### Getting Started

This project utilizes the [Kedro Framework](https://docs.kedro.org/en/stable/index.html) to streamline data processing and analysis. The code is structured into three main tasks: Data Processing, LST counterfactual modelling and Ecosystem service calculation.



1. **Docker | environment set up**

    The project is set up to run in a Docker container. Documentation on how to build and run the Docker image for this project can be found [here](https://ac-willeke.github.io/urban-climate/html/docker.html).

    Note that the containter contains no data, so please ensure to mount your data into the container.


2. **Kedro | run the pipelines**

    The project contains three Kedro pipelines:
    - **raster_processing**: loads and processes the input data
    - **lst_counterfactual**: runs the LST counterfactual modelling
    - **ecosystem_services**: calculates the ecosystem services

    The pipelines can be run using the following commands:
    ```bash
    # Enter the container
    docker run -it <image-name>:<image-tag>
    ``````

    ```python
    kedro run --pipeline=raster_processing
    kedro run --pipeline=lst_counterfactual
    kedro run --pipeline=ecosystem_services
    ```

    The pipelines can also be run in sequence using the following command:

    ```python
    kedro run
    ```

    The pipeline dependencies are visualized in the following graph:

    ![Pipeline Dependencies](/docs/source/img/pipeline_dependencies.png)







Code is provided for the following tasks:

1. **Raster Preparation**
    - load signle GeoTIFFs into a rasterstack
    - convert a raster stack to a GeoDataFrame, where each band becomes a colum, and each pixel value is a row.

    ```python
    kedro run --pipeline=raster_processing
    kedro viz --pipeline=raster_processing
    ```

2. **Data Analysis**
    - Land Surface Temperature counterfactual modelling
    - Ecosystem service calculation




## Documentation
Documentation is hosted on GitHub Pages: [Urban Climate Documentation](https://ac-willeke.github.io/urban-climate/html/index.html)


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

## Contributors

Willeke A'Campo (NINA), willeke.acampo@nina.no

Zander Venter (NINA), zander.venter@nina.no
