from functools import wraps
from collections import namedtuple
import weakref
import os

from .annotate import local

class FlId(namedtuple('FlId', 'name, long, id, pid')):
    """ flow identity for an object """
    def __eq__(self, other):
        if not isinstance(other, FlId):
            return NotImplemented
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @property
    def master(self):
        return self.pid == os.getpid()

    def __str__(self):
        return '*'+self.name

    def __repr__(self):
        return '*'+self.long


class Sym(str):
    """ simple string symbol with same interface as FlId """
    @property
    def id(self):
        return id(self)

    @property
    def long(self):
        return self

    name = long


class Fled:
    """ mixin to add flow identity to an object """
    __name__ = None
    __master__ = False

    def __init__(self, *args, name=None, **kws):
        super().__init__(*args, **kws)

        if self.__master__:
            id_ = str(os.getpid())
        else:
            id_ = '{:x}'.format(id(self))

        if name is None:
            name = type(self).__name__
        long = '{}-{}'.format(name, id_)

        self.id = FlId(name, long, id(self), os.getpid())

    def __str__(self):
        return str(self.id)[1:]

    def __repr__(self):
        return repr(self.id)[1:]


class Spaced(Fled):
    """ object with a combound namespace """
    __by__ = []

    @local
    def __namespace__(self):
        space = '/'.join([getattr(self,by).__namespace__ for by in self.__by__] + [self.id.long])
        os.system("mkdir -p '%s'" % space)
        return space


def ctxmethod(f):
    @wraps(f)
    def insert_ctx(self, *args, **kws):
        return f(self, *args, ctx=self.__fl__.ctx, **kws)
    return insert_ctx


