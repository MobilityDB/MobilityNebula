# syntax=docker/dockerfile:1
# NebulaStream's CI image with the libraries libmeos links when MobilityDB builds it with every
# family (-DMEOS=ON -DALL=ON): GEOS, PROJ and json-c always, GSL, H3 for the H3 family and GDAL for
# raster. libmeos itself is built in this image from a MobilityDB commit and installed in a prefix
# the build names through MEOS_ROOT.
ARG IMAGE
FROM ${IMAGE}

USER root
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        libgeos-dev \
        libproj-dev \
        libjson-c-dev \
        libgsl-dev \
        libh3-dev \
        libgdal-dev \
    && rm -rf /var/lib/apt/lists/*
