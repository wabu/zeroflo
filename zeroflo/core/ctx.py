from .idd import *
from .topology import Topology
from .control import Control

import asyncio
import aiozmq

from functools import wraps
from contextlib import contextmanager

from pyadds import spawn


def setup_aiozmq():
    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    pass

class Context:
    __active__ = None
    def __init__(self, name=None):
        self.name = name
        self.store['ctx'] = self.bound['ctx'] = self
        self.store['tp'] = tp = Topology(name=name)
        self.store['ctrl'] = ctrl = Control(ctx=self, tp=tp)

    @cached
    def store(self):
        return {}

    @cached
    def bound(self):
        return {}

    @property
    def topology(self):
        try:
            return self.bound['tp']
        except KeyError:
            raise ValueError("flow topology is only accessable inside setup() contex")

    @property
    def control(self):
        try:
            return self.bound['ctrl']
        except KeyError:
            raise ValueError("flow control is only accessable inside run() context")

    def register(self, unit):
        u = self.store['tp'].register(unit)
        self.store['ctrl'].register(unit)
        return u

    @cached
    def spawner(self):
        spawner = spawn.get_spawner('forkserver')
        return spawner

    @contextmanager
    def activate(self, key):
        if Context.__active__ != None:
            raise ValueError('another context is already active!')
        try:
            self.bound[key] = self.store[key]
            Context.__active__ = self.bound
            yield
        finally:
            self.bound.pop(key, None)
            Context.__active__ = None


    @contextmanager
    def setup(self, setup=None):
        if setup:
            setup()
        #setup_aiozmq()

        #self.spawner.add_setup(setup_aiozmq)
        self.spawner.add_setup(setup)

        with self.activate('tp'):
            yield

    @contextmanager
    def run(self):
        with self.activate('ctrl'):
            yield


def get_active(key='ctx'):
    if Context.__active__ is None:
        raise ValueError("No current context, "
                "use the `flo.context(...)` context manaanger before creating "
                "flow units or pass a ctx to the unit constructor.")
    try:
        return Context.__active__[key]
    except KeyError:
        raise ValueError("%s not inside current context" % key)


def with_active(key):
    def annotate(f):
        @wraps(f)
        def add_arg(*args, **kws):
            if not key in kws:
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
