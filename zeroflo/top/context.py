import tempfile
from functools import wraps
from contextlib import contextmanager

from pyadds.annotate import delayed

from . import topology


__context__ = None


class Context:
    def __init__(self, name='flo'):
        self.name = name

    @delayed
    def root(self):
        return tempfile.TemporaryDirectory(
            prefix='__'+self.name+'-', suffix='__', dir='.')

    @delayed
    def topology(self):
        return topology.Topology()

    @contextmanager
    def bind(self):
        global __context__
        try:
            prev = __context__
            __context__ = self
            yield self
        finally:
            __context__ = prev


def with_ctx(f):
    @wraps(f)
    def injecting(*args, ctx=None, **kws):
        try:
            global __context__
            if ctx is None:
                if __context__ is None:
                    __context__ = Context()
                ctx = __context__
            return f(*args, ctx=ctx, **kws)
        finally:
            pass
    return injecting
