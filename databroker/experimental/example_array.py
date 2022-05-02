import numpy
from tiled.client import from_uri

from client_ext import submit_array

from queries import scan_id

c = from_uri("http://localhost:8000/api")

# placeholders for reconstructions
# Numpy arrays of any dimensionality, shape, or dtype
# (other than Python "object" type) are accepted.
recon1 = numpy.ones((5, 5, 5))

submit_array(
    c.context, recon1, {"scan_id": 1, "method": "A"}, ["BlueskyNode"], "image/png"
)

print("searching for reconstructions corresponding to scan_id 1...")
results = c.search(scan_id(1))
print(f"found {len(results)} results")
print("first result:")
result = results.values_indexer[0]
print("array:", result[:])  # numpy array
breakpoint()
print("metadata:", result.metadata)  # dict of metadata
