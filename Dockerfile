FROM ghcr.io/bluesky/tiled:0.2.11 as base

USER root
COPY . /databroker-src/
RUN python -m ensurepip
RUN python -m pip install "/databroker-src[back-compat,server]"
USER app
