from functools import wraps
from databroker.databroker import fill_event


def nonscalar_example(func):
    @wraps(func)
    def fill_all_events(*args, **kwargs):
        events = func(*args, **kwargs)
        for event in events:
            print(event)
            fill_event(event)
        return events
    return fill_all_events
