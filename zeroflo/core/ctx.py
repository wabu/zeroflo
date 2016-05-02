from .topology import Topology
from .control import Control
from pyadds.annotate import cached

import logging
from functools import wraps
from contextlib import contextmanager

from pyadds import spawn


def setup_logging():
    logging.basicConfig(format='%(levelname)-7s %(message)s'
                               '\n        \\\\ '
                               '%(asctime)s | '
                               '%(levelname).1s:%(levelno)2d | '
                               '%(processName)s | %(name)s')
    logging.getLogger().setLevel("INFO")
    logging.getLogger('asyncio').setLevel("WARNING")


class CallList(list):
    def __call__(self, *args, **kws):
        return [f(*args, **kws) for f in self]


class Context:
    __active__ = None
    __previous__ = None

    def __init__(self, name=None, setup=None):
        self.setups = CallList()
        if setup is None:
            setup = setup_logging

        self.name = name
        self.ctx = self
        self.topology = Topology(name=name)
        self.control = Control(ctx=self, tp=self.topology)
        self.setups.append(setup)
        self.managed = {'ctx': self,
                        'tp': self.topology,
                        'ctrl': self.control}
        if setup:
            setup()

        self.activate(active='return')

    def activate(self, active='raise'):
        if Context.__active__ and Context.__active__ != self:
            if active == 'raise':
                raise ValueError('another zeroflo context already active')
            elif active == 'return':
                return self
            elif active == 'ignore':
                pass
            else:
                raise TypeError("active argument should be 'raise' or 'ignore'")
        Context.__active__ = self
        return self

    def deactivate(self):
        Context.__previous__ = Context.__active__
        Context.__active__ = None

    def register(self, unit):
        u = self.topology.register(unit)
        self.control.register(unit)
        return u

    @cached
    def spawner(self):
        spawner = spawn.get_spawner('forkserver')
        spawner.add_setup(self.setups)
        return spawner

    def __enter__(self):
        return self.activate()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.deactivate()

    def setup(self, setup=None):
        self.setups.append(setup)
        setup()
        return self

    @contextmanager
    def run(self):
        try:
            self.activate()
            yield self
        finally:
            self.deactivate()
            self.control.shutdown()


def get_active(key='ctx'):
    if Context.__active__ is None and Context.__previous__ is None:
        raise ValueError(
            "No current context, "
            "use the `flo.Context(...)` context manaanger before creating "
            "flow units or pass a ctx to the unit constructor.")
    try:
        return (Context.__active__ or Context.__previous__).managed[key]
    except KeyError:
        raise ValueError("%s not inside current context" % key)


def with_active(key):
    def annotate(f):
        @wraps(f)
        def add_arg(*args, **kws):
            if key not in kws:
                try:
                    kws[key] = get_active(key)
                except ValueError:
                    pass
            return f(*args, **kws)
        return add_arg
    return annotate

withtp = with_active('tp')
withctrl = with_active('ctrl')
withctx = with_active('ctx')
