# bluesky-tiled-plugins

This project provides a set of tools that facilitate the integration of Bluesky,
a data acquisition framework, with Tiled, a data management system.

These plugins enable seamless writing of Bluesky data to Tiled and additionally provide
a set of specialized Python clients to facilitate the retrieval of Bluesky data and
enhance the overall data handling experience for users.

For a user wishing to connect to a running Tiled server and access Bluesky data,
this package, along with its dependency `tiled[client]`, is all they need.

The databroker package is only required if the user wants to use the legacy
`databroker.Broker` API.
