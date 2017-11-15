from __future__ import absolute_import, division, print_function

from glue.core.data import Data, Component


def read_header(header):
    out = []
    for stream in header.stream_names:
        result = Data(label="{stream}_{uid}".format(stream=stream, uid=header.start['uid']))
        tbl = header.table(stream)
        for col in tbl:
            result.add_component(Component(tbl[col].values), col)
        out.append(result)

    return out
