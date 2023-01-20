# to build locally, use:
#  docker build -t databroker . -f ./docker/Dockerfile

FROM ghcr.io/bluesky/tiled:v0.1.0a80 as base

FROM base as builder

WORKDIR /build
COPY . .
RUN pip install .

FROM base as app
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
