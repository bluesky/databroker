from databroker.broker import simple_broker
from databroker.muggler import data
hdr = simple_broker.get_last_headers()
ev = simple_broker.get_events_by_run(hdr, None)
dm = data.DataMuggler.from_events(ev)
dm.col_info
