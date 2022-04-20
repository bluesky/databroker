import numpy
import pandas

from tiled.client import from_uri

from client_ext import submit_df_recon

# from queries import scan_id

c = from_uri("http://localhost:8000/api")

recon1 = numpy.ones((5, 5))

data = {"Column1": recon1[0],
        "Column2": recon1[1],
        "Column3": recon1[2],
        "Column4": recon1[3],
        "Column5": recon1[4]}

dataframe = pandas.DataFrame(data)

meta={}
for key,value in dataframe.items():
    meta[key] = value.dtypes.name
    
submit_df_recon(c.context, dataframe, meta, ["BlueskyNode"], "image/png")