from functools import wraps
from collections import namedtuple
import weakref
import os

from .annotate import local, shared, Named

class Id(namedtuple('FlId', 'name, long, id, ref, pid'), Named):
    """ flow identity for an object """
    def __init__(self, name, long, id, ref, pid):
        pass

    @property
    def master(self):
        return self.pid == os.getpid()

    def __eq__(self, other):
        if not isinstance(other, Id):
            return NotImplemented
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return '*'+self.name

    def __repr__(self):
        return '*'+self.long


class Sym(Id):
    """ simple string symbol with same interface as Id """
    def __new__(cls, str, ref):
        return super().__new__(cls, str, str, hash(str), ref, os.getpid())

    def __init__(self, str, ref):
        pass


class Derived(Id):
    def __new__(cls, id, name, *args):
        if args:
            return super().__new__(cls, id, name, *args)
        else:
            return super().__new__(cls, name, '{}.{}'.format(id.long, name), (id, name), ref=name, pid=os.getpid())

    def __init__(cls, id, name):
        pass

    @property
    def base(self):
        return self.id[0]


class Idd(Named):
    """ 
    Mixin to add identity to an object.

    The identity may be local to some other namespace, 
    for global identification one can use the `Pointed` mixin.
    """
    __ref__ = None
    __master__ = False

    def __init__(self, *args, name=None, id_=None, **kws):
        if not type(self).__ref__:
            type(self).__ref__ = type(self).__name__

        if name is None:
            name = id_.name if id_ else type(self).__ref__

        super().__init__(*args, name=name, **kws)

        if not id_:
            if self.__master__:
                id_ = str(os.getpid())
            else:
                id_ = '{:x}'.format(id(self))
            long = '{}-{}'.format(name, id_)
            id_ = Id(name, long, id(self), self.__ref__, os.getpid())

        self.id = id_

    def __str__(self):
        return str(self.id)[1:]

    def __repr__(self):
        return repr(self.id)[1:]


class Path:
    """ point specified by multiple ids """
    def __init__(self, *ids):
        self.ids = ids
        self.byname = {id.ref: id for id in ids}

    def __getitem__(self, name):
        return self.byname.get(name)

    @local
    def names(self):
        return tuple([id.long for id in self.ids])

    @local
    def namespace(self):
        os.system("mkdir -p '%s'" % self)
        return '/'.join(self.names)

    def __eq__(self, other):
        if not isinstance(other, Path):
            return NotImplemented
        return self.ids == other.ids

    def __hash__(self):
        return hash(self.ids)

    def __str__(self):
        return '/'.join(self.names)

    __repr__ = __str__


class IddPath(Idd):
    """ object with globally identifying id path """
    __by__ = [...]

    @shared
    def path(self):
        ids = []
        for by in self.__by__:
            if by == ...:
                ids.append(self.id)
            else:
                obj = getattr(self, by)
                if isinstance(obj, IddPath):
                    ids.extend(obj.path.ids)
                elif isinstance(obj, Idd):
                    ids.append(obj.id)
                elif isinstance(obj, Id):
                    ids.append(obj)
                elif isinstance(obj, str):
                    ids.append(Sym(obj, by))
                else:
                    raise TypeError("Can't infer path-id for %s of %s (%s)" 
                                    % (by, type(self).__name__, type(obj).__name__))
        return Path(*ids)


class WithPath:
    """ object with explicitly passed path """
    def __init__(self, *, path):
        self.path = path


def ctxmethod(f):
    @wraps(f)
    def insert_ctx(self, *args, **kws):
        return f(self, *args, ctx=self.__ctx__.ctx, **kws)
    return insert_ctx

