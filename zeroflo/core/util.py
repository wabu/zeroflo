from functools import wraps
from collections import namedtuple
import weakref
import os

class FlId(namedtuple('FlId', 'name, long, id')):
    """ flow identity for an object """
    def __eq__(self, other):
        if not isinstance(other, FlId):
            return NotImplemented
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return '*'+self.name

    def __repr__(self):
        return '*'+self.long


class Sym(str):
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

        self.id = FlId(name, long, id(self))

    def __str__(self):
        return str(self.id)[1:]

    def __repr__(self):
        return repr(self.id)[1:]


class Named:
    def __init__(self, name):
        self.name = name

class Annotate(Named):
    def __new__(cls, definition):
        return wraps(definition)(super().__new__(cls))

    def __init__(self, definition):
        super().__init__(name=definition.__name__)
        self.definition = definition

class Descr(Named):
    def lookup():
        raise NotImplementedError

class ObjDescr(Descr):
    def __init__(self, name):
        super().__init__(name=name)
        self.entry = '_'+name

    def lookup(self, obj):
        return obj.__dict__, self.entry

class RefDescr(Descr):
    def __init__(self, name):
        super().__init__(name)
        self.refs = weakref.WeakKeyDictionary()

    def lookup(self, obj):
        return self.refs, obj

class Get(Descr):
    def __get__(self, obj, objtype):
        if obj:
            dct,key = self.lookup(obj)
            return dct[key]
        else:
            return self

class Set(Descr):
    def __set__(self, obj, value):
        if obj:
            dct,key = self.lookup(obj)
            dct[key] = value
            return value
        else:
            return self

    def __delete__(self, obj):
        dct,key = self.lookup(obj)
        dct.pop(key, None)


class Cached(Descr):
    def __get__(self, obj, objtype):
        if obj is None:
            return self
        dct,key = self.lookup(obj)
        try:
            return dct[key]
        except KeyError:
            value = self.definition(obj)
            dct[key] = value
            return value


class shared(Annotate, Cached, ObjDescr, Get):
    pass

class local(Annotate, Cached, RefDescr, Set, Get):
    pass 

def ctxmethod(f):
    @wraps(f)
    def insert_ctx(self, *args, **kws):
        return f(self, *args, ctx=self.__fl__.ctx, **kws)
    return insert_ctx
