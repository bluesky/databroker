from __future__ import absolute_import, division, print_function

from glue.core.data import Data


def read_header(header):
    out = []
    for stream in header.stream_names:
        result = Data(label="{stream}_{uid}".format(stream=stream, uid=header.start['uid']))
        tbl = header.table(stream, fill=True)
        for col in tbl.columns:
            result.add_component(tbl[col], str(col))
        out.append(result)

    return out
