import numpy
import pandas

from tiled.client import from_uri

from client_ext import submit_dataframe

from queries import scan_id

c = from_uri("http://localhost:8000/api")

recon1 = numpy.ones((5, 5))

data = {
    "Column1": recon1[0],
    "Column2": recon1[1],
    "Column3": recon1[2],
    "Column4": recon1[3],
    "Column5": recon1[4],
}

dataframe = pandas.DataFrame(data)

submit_dataframe(
    c.context, dataframe, {"scan_id": 1, "method": "A"}, ["BlueskyNode"], "image/png"
)

print("searching for reconstructions corresponding to scan_id 1...")
results = c.search(scan_id(1))
print(f"found {len(results)} results")
print("first result:")
result = results.values_indexer[0]
print("dataframe:", result.read())  # dataframe
print("metadata:", result.metadata)  # dict of metadata
