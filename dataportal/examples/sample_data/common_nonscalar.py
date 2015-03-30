from functools import wraps
from dataportal.broker.simple_broker import fill_event


def nonscalar_example(func):
    @wraps(func)
    def fill_all_events(*args, **kwargs):
        events = func(*args, **kwargs)
        for event in events:
            fill_event(event)
        return events
    return fill_all_events
