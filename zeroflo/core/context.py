from contextlib import contextmanager
from .idd import *
from .topology import Topology
from .control import Control

class Context(Idd):
    def __init__(self, name=None):
        super().__init__(name=name)

    @cached
    def tp(self):
        return Topology()

    @cached
    def ctrl(self):
        return Control(ctx=self, tp=self.tp)

    def register(self, unit):
        self.tp.register(unit)
        self.ctrl.register(unit)

__context__ = None

def get_current_context():
    if __context__ is None:
        raise ValueError("No current context, "
                "use the `flo.context(...)` context manaanger before creating "
                "flow units or pass a ctx to the unit constructor.")
    return __context__


@contextmanager
def context(ctx=None, *args, **kws):
    global __context__

    if __context__:
        raise ValueError("Another flow context is already active")

    if not isinstance(ctx, Context):
        ctx = Context(name=ctx, *args, **kws)

    try:
        __context__ = ctx
        yield __context__
    finally:
        __context__ = None

