# Use the latest gdal image as base image
ARG BASE_IMAGE=osgeo/gdal:ubuntu-small-latest
FROM $BASE_IMAGE as runtime-environment

# Update, install pip and git, and clean up in one step
RUN apt-get update && \
    apt-get -y install python3-pip git --fix-missing && \
    apt-get install -y pandoc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install project requirements and remove the requirements file in one step
COPY src/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

# Install the required locale
RUN apt-get update && \
    apt-get install -y locales && \
    locale-gen nb_NO.UTF-8

# Set the locale
ENV LC_ALL=nb_NO.UTF-8
ENV LANG=nb_NO.UTF-8

# Set workdir to vs code workspace
WORKDIR /workspaces/urban-climate

#
FROM runtime-environment

# Copy the whole project except what is in .dockerignore
# Ensure that /data is not copied into the image
# Mount /data as volume (see devcontainer.json for mount config)
COPY . .

# SPHINX documentation config
WORKDIR /workspaces/urban-climate/docs
RUN sphinx-apidoc --module-first -o source ../src/urban_climate
RUN pip install -e ../src
WORKDIR /workspaces/urban-climate

EXPOSE 8888

CMD ["kedro", "run"]
