
Environment Set Up and Installation
===================================

**Dependencies:**
- Ubuntu
- Docker
- VS Code
- VS Code Extension: Dev Containers
- VS Code Extension: Docker
- VS Code Extension: Live Server

## Create project from template

1. Click on the green button "Use this template" on GitHub.
2. Open VS Code and clone the repository.
3. Set up your docker configuration files:
    - check your `Dockerfile`
    - check your `.devcontainer.json`
        - mount your local/network folders
        - define vscode extensions
4. Build a new docker image using the Dockerfile:

    ```bash
    # build docker image
    docker build .
    docker build -t <image-name>:<image-tag> .

    # list docker images
    docker images

    # enter docker image
    docker run -it <image-id>

    # remove docker image
    docker rmi <image-id>
    ```
5. Start your docker container using VS Code
    - open the project folder in VS Code
    - open the command palette (Ctrl+Shift+P)
    - select "Dev Containers: Reopen in Container"
    --> this uses the .devcontainer.json file to start the container

6. Install your local package
    ```bash
    cd src
    pip install -e .
    ```

7. Install pre-commit and linters outside your container using pipx
    ```bash
    pipx install pre-commit
    pipx install black
    pipx install black
    pipx install ruff
    pipx install pyment
    ```

    ** pre-commit** checks certain actions before committing changes to your repository. The list of actions that are executed are defined in `.pre-commit-config.yaml`.
    - Install `pre-commit`: `pipx install pre-commit`
    - Enter into your git repository and install the hooks: `pre-commit install` (optional, but recommended)
    - de-activate pre-commit:
        - delete or comment out the lines in .pre-commit-config.yaml
        - `pre-commit uninstall`
        - `pre-commit clean`

**Your Environment is now set up and ready to go! Next set up your [project](project_setup.md).**

## Create project from scratch

Setup Kedro Project using venv. Follow the instructions from [Kedro's official documentation.](https://docs.kedro.org/en/stable/get_started/install.html)

1. Create Kedro project directory:
    ```bash
    mkdir urban-climate && cd urban-climate
    ```
2. Create and activate a new virtual environment
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```
3. Install Kedro inside the venv
    ```bash
    pip install kedro
    # check installation
    kedro info
    ```
4. Create a new Kedro project
    ```bash
    kedro new
    ```
5. Install the project dependencies
    ```bash
    cd %project_name%
    pip install -r src/requirements.txt
    ```
6. Install Docker pluging for Kedro
    ```bash
    kedro plugin install kedro-docker
    ```
7. Create a Dockerfile
    ```bash
    kedro docker init
    ```
8. Edit the Dockerfile to meet your project needs
    - replace base image from `python:3.8-slim-buster` to `osgeo/gdal:ubuntu-small-latest`

    ```bash
    ARG BASE_IMAGE=osgeo/gdal:ubuntu-small-latest
    FROM $BASE_IMAGE as runtime-environment
    ```
    - remove user creation from Dockerfile
    - update workdir to vscode workspace
    - ensure that data is not copied to the docker image

9. Build docker image

    --> creates .devcontainer.json file
    ```bash
    kedro docker build -t name:tag
    ```
10. Update your .devcontainer.json file to you personal config settings
    - mount your local config and data folder
    - define vscode extensions

    Example: `template.devcontainer.json`

11. Start your docker container using VS Code
    - open the project folder in VS Code
    - open the command palette (Ctrl+Shift+P)
    - select "Dev Containers: Reopen in Container"
    --> this uses the .devcontainer.json file to start the container

12. Install your dependencies
    - install using pip (no venv is necessary as docker is isolated)
    - update requirements.txt `pip freeze > src/requirements.txt`

12. Close the container
    - open the command palette (Ctrl+Shift+P)
    - select "Dev Container: Reopen Folder locally"

13. Set up sphinx documentation
    - see [kedro docs](https://docs.kedro.org/en/stable/tutorial/package_a_project.html)
    - open container
    - backup your docs folder created by kedro
    - delete `/docs/source/conf.py` and `/docs/source/index.rst`
    - init sphinx (separate source/build folders)
    ```bash
    sphinx-quickstart docs
    ```
    - replace `conf.py` and `index.rst` with your backuped files.
    - init module docstring documentation
    ```bash
    sphinx-apidoc --module-first -o source ../src/<package_name>
    ```
    - from docs folder run `pip install -e ../src`

14. Make sphinx compatible with GitHub Pages
    - [medium blog](https://python.plainenglish.io/how-to-host-your-sphinx-documentation-on-github-550254f325ae)
    - update BUILDDIR to ../docs in make.bat and Makefile
    - add `.nojekyll` file to docs folder
    - build html: `make html`
    ```bash
    --docs
        |--doctrees
        |--html
            |--index.html
            |..
        |--source
            |-- conf.py
            |-- index.rst
            |..
        |--make.bat
        |--Makefile
    ```
    - open `docs/index.html` in browser
        - right click on index.html and select "Open with Live Server"


14. Init Git Repository and Publish to Github

15. Publish Sphinx Documentation to GitHub Pages
    **optional, you can also publish from main**
    - create a new branch `gh-pages`
    - add `docs/build/html` to `.gitignore`
    - add `docs/build/html` to `gh-pages` branch
    - commit and push changes to `gh-pages` branch
    - go to GitHub > settings > Pages
        - select `gh-pages` branch as source for GitHub Pages
        - select `docs/` as folder
    - open `https://<username>.github.io/<project_name>/` in browser

**Your Environment is now set up and ready to go! Next set up your [project](./project_setup.md).**
