Local Climate Regulation
========================

[![Documentation Status](https://readthedocs.org/projects/kedro-geospatial/badge/?version=latest)](https://ac-willeke.github.io/kedro-geospatial/html/index.html)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


This repository provides a workflow for analyzing the impact of urban trees on local climate regulation. This workflow is an adapted version of the methodolgy described in [Venter et al. (2020)](https://www.sciencedirect.com/science/article/pii/S0048969719361893).

Code is provided for the following tasks:

0. **Extract Raster Data**
    - `extract_raster_data.js`
    - extract raster data from GEE

1. **Raster Processing**
    - kedro pipeline: `raster_processing`
    - load single GeoTIFFs into a rasterstack
    - convert a raster stack to a GeoDataFrame, where each band becomes a colum, and each pixel value is a row.

2. **Land Surface Temperature (LST) counterfactual modelling**
    - kedro pipeline: `lst_counterfactual`
    - calculate the LST counterfactual for each pixel in the study area

3. **Ecosystem service calculation**
    - kedro pipeline: `ecosystem_services`
    - calculate the ecosystem services per district

Complete documentation is hosted on GitHub Pages: [Urban Climate Documentation](https://ac-willeke.github.io/urban-climate/html/index.html)


## Getting Started
This project utilizes the [Kedro Framework](https://docs.kedro.org/en/stable/index.html) to streamline data processing and analysis. See the documentation for detailed step by step instricutions on how to set up the project and run the project.

1. **Docker | environment set up**

    The project is set up to run in a Docker container. Documentation on how to build and run the Docker image for this project can be found [here](https://ac-willeke.github.io/urban-climate/html/docker.html).

    ```bash
    # Build container from Dockerfile
    docker build -t <image-name>:<image-tag> .
    ``````

    Note that the containter contains no data, so please ensure to mount your data into the container (see `template_devcontainer.json`).


2. **Kedro | run the pipelines**

    The project contains three Kedro pipelines:
    - **raster_processing**: loads and processes the input data
    - **lst_counterfactual**: runs the LST counterfactual modelling
    - **ecosystem_services**: calculates the ecosystem services

    The pipelines can be run using the following commands:
    ```bash
    # Enter the container
    docker run -v /host/directory:/container/directory:cached <image-id> kedro run --pipeline=<pipeline-name>

    ``````

    ```python
    # Kedro commands
    kedro run --pipeline=raster_processing
    kedro run --pipeline=lst_counterfactual
    kedro run --pipeline=ecosystem_services
    ```

    The pipelines can also be run in sequence using the following command:

    ```python
    kedro run
    ```

3. **Visualize the workflow**

    The workflow can be visualized using the Kedro Viz tool. This tool can be accessed by running `kedro viz` in the terminal.

    ![Kedro Viz](/docs/source/img/kedro_viz.png)

----------------

### **References**
- Venter, Z.S., et al. (2020) Linking green infrastructure to urban heat and human health risk mitigation in Oslo, Norway. Science of The Total Environment, 709, 136193. https://doi.org/10.1016/j.scitotenv.2019.136193

### **Contributors**

- Willeke A'Campo (NINA), willeke.acampo@nina.no

- Zander Venter (NINA), zander.venter@nina.no

### **Acknowledgments**

*This repository is part of the project:*

**TREKRONER Prosjektet** | Trærs betydning for klimatilpasning, karbonbinding, økosystemtjenester og biologisk mangfold.

----------------
