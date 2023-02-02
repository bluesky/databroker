FROM ghcr.io/bluesky/tiled:v0.1.0a83 as base

FROM base as builder

# We need git at build time in order for versioneer to work. This
# does not leak out of the API (at this time) but it seems useful
# to have it correctly reported in the build logs.
RUN apt-get -y update && apt-get install -y git

WORKDIR /code

# Copy requirements over first so this layer is cached and we don't have to
# reinstall dependencies when only the databroker source has changed.
COPY requirements-server.txt /code/
RUN pip install --upgrade --no-cache-dir pip wheel
RUN pip install --upgrade --no-cache-dir -r /code/requirements-server.txt

COPY . .
# note requirements listed here but all deps should be already satisfied
RUN pip install .[server]

FROM base as runner

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
