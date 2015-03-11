"""Module for Enaml widgets that are generally useful"""
from enaml.widgets.api import PushButton, Timer
from atom.api import Typed, observe, Event
from enaml.core.declarative import d_
from enaml.layout.api import (grid, align)

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

def save_state(history, history_key, state):
    state = {k: v for k, v in state.items() if k not in non_stateful_attrs}
    history.put(history_key, state)


