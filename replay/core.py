"""Module for Enaml widgets that are generally useful"""
from enaml.widgets.api import PushButton, Timer
from atom.api import Typed, observe, Event
from enaml.core.declarative import d_
from enaml.layout.api import (grid, align)
import json
from metadatastore.api import Document

class ProgrammaticButton(PushButton):
    clicked = d_(Event(bool), writable=True)
    toggled = d_(Event(bool), writable=True)


class TimerButton(ProgrammaticButton):
    timer = d_(Typed(Timer))
    checkable = True

    @observe('checked')
    def checked_changed(self, changed):
        if self.checked:
            self.timer.start()
        else:
            self.timer.stop()


def generate_grid(container, num_cols):
    """ Generate grid constraints with given number of columns.

    Notes
    -----
    Shamelessly copied from enaml/examples/layout/advanced/factory_func.enaml
    """
    rows = []
    widgets = container.visible_widgets()
    row_iters = (iter(widgets),) * num_cols
    rows = list(zip(*row_iters))
    return [grid(*rows, row_spacing=0, column_spacing=0, row_align='v_center',
                 column_align='h_center'),
            align('width', *widgets)]


non_stateful_attrs = ['history']
non_stateful_types = [Document]


def save_state(history, history_key, state, sanitize=False, blacklist=True):
    """Helper function that saves the state of atom objects

    When discussing state and 'Atom' objects, there are two built-in
    helper functions: __getstate__ and __setstate. __getstate__ returns
    a dictionary keyed on the names of all 'members' of the atom object,
    where 'members' are the strongly-typed Attributes defined at the
    class level

    Note:

    Parameters
    ----------
    history : dataportal.replay.persist.History
        An instance of the History object which wraps a sql db
    history_key : str
        The primary key used to set and retrieve state
    state : dict
        Dictionary of state that should be saved to the db
    sanitize : bool, optional
        defaults to False
        True: Strips values from `state` that cannot be serialized
    blacklist : bool, optional
        defaults to True
        True: remove keys from the state dictionary that appear in the
            `non_stateful_attrs` module-level list
    """
    if blacklist:
        # remove keys that are not helpful for
        state = {k: v for k, v in state.items() if not (k in non_stateful_attrs
                 or isinstance(v, tuple(non_stateful_types)))}
    if sanitize:
        # remove objects that cannot be serialized
        json.loads(json.dumps(state, skipkeys=True))
    # save to db
    history.put(history_key, state)


