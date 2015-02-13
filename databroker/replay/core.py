"""Module for Enaml widgets that are generally useful"""
from enaml.widgets.api import PushButton, Timer
from atom.api import Typed, observe, Event
from enaml.core.declarative import d_

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
