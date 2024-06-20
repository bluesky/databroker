# bluesky-tiled-plugins

This is a separate Python package, `bluesky-tiled-plugins`, that is
developed in the databroker repository.

For a user wishing to connect to a running Tiled server and access Bluesky data,
this package, along with its dependency `tiled[client]`, is all they need.

The databroker package is only required if the user wants to use the legacy
`databroker.Broker` API.
