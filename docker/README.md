## These commands need to be run from the databroker package root directory.

## Build image from dockerfile
`docker build -t databroker . -f ./docker/Dockerfile`
or
`sudo podman build -t databroker . -f ./docker/Dockerfile`

## Start container:
`docker run --rm -p 8000:8000 --mount type=bind,source="$(pwd)",target=/deploy --env TILED_CONFIG=/deploy/docker/example_config.yml databroker`
or
`podman run --rm -p 8000:8000 --mount type=bind,source="$(pwd)",target=/deploy --env TILED_CONFIG=/deploy/docker/example_config.yml databroker`
