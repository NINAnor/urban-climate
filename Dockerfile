# build image from dockerfile:
# docker build -t kedro-geospatial:1.0 .

# use the latest gdal image as base image
ARG BASE_IMAGE=osgeo/gdal:ubuntu-small-latest
FROM $BASE_IMAGE as runtime-environment

# install pip
RUN apt-get update && apt-get -y install python3-pip --fix-missing

# install project requirements in to the /tmp dir of the docker image
COPY src/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

# Set the locale
ENV LC_ALL=nb_NO.UTF-8
ENV LANG=nb_NO.UTF-8

# set workdir to vs code workspace
WORKDIR /workspaces/urban-climate

#
FROM runtime-environment

# copy the whole project except what is in .dockerignore
# ensure that /data is not copied into the image
# mount /data as volume (see devcontainer.json for mount config)
COPY . .

EXPOSE 8888

CMD ["kedro", "run"]
