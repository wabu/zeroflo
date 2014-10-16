from pyadds.annotate import Named, delayed, cached
from pyadds.str import name_of

from collections import namedtuple
from pathlib import Path
from os.path import join as join_path

import traceback

class Id(namedtuple('IdBase', 'name, idd')):
    def __str__(self):
        return self.name

    def __repr__(self):
        return '{}-{:x}'.format(self.name, self.idd)

    def __hash__(self):
        return hash(self.idd)

    def __eq__(self, other):
        if isinstance(other, Id):
            return self.idd == other.idd
        else:
            return NotImplemented

class NilId(Id):
    def __new__(cls):
        return super().__new__(cls, 'nil', 0)

    def __bool__(self):
        return False

Nil = NilId()


class Sym(namedtuple('SymBase', 'idd'), Id):
    @property
    def name(self):
        return self.idd

    def __str__(self):
        return self.idd

    __repr__ = __str__


class Named(namedtuple('NamedBase', 'name, value')):
    def __str__(self):
        return '{}:{}'.format(name, value)

    __repr__ = __str__


class IdPath(namedtuple('IdPathBase', 'refs, ids')):
    @property
    def strs(self):
        return tuple(map(repr, self.ids))

    @property
    def hier(self):
        return '.'.join(self.strs)

    @property
    def path(self):
        return Path(*self.strs)

    def namespace(self):
        path = self.path
        if not path.is_dir():
            path.mkdir(parents=True)
        return str(path)

    def prefixed(self, prefix):
        return IdPath(tuple(prefix+n for n in self.refs), self.ids)

    def __getitem__(self, item):
        try:
            refs,ids = self
            return ids[refs.index(item)]
        except ValueError:
            return super().__getitem__(item)

    def sub(self, start=None, stop=None):
        return IdPath(self.refs[start:stop], self.ids[start:stop])


    def __add__(self, other):
        if isinstance(other, IdPath):
            return IdPath(self.refs+other.refs, self.ids+other.ids)
        if isinstance(other, Named):
            return self + IdPath((other.name,), (Sym(str(other.value)),))
        return NotImplemented

    def __str__(self):
        return join_path(*self.strs)

    __repr__ = __str__


class Idd:
    __by__ = None
    def __init__(self, *args, name=None, idd=None, by=None, **kws):
        self.id = Id(name or name_of(self), 
                     idd or id(self))

        by = by or self.__by__
        if not by:
            by = (..., )
        if not isinstance(by, tuple):
            by = (by, ...)
        self.by = by

    @cached 
    def path(self):
        refs = []
        ids = []
        for by in self.by:
            if by == ...:
                refs.append(name_of(self))
                ids.append(self.id)
            else:
                obj = getattr(self, by)
                if obj is None:
                    refs.append(by)
                    ids.append(Nil)
                elif isinstance(obj, Idd):
                    refs.extend(obj.path.refs)
                    ids.extend(obj.path.ids)
                elif isinstance(obj, IdPath):
                    refs.extend(obj.refs)
                    ids.extend(obj.ids)
                elif isinstance(obj, Id):
                    refs.append(by)
                    ids.append(obj)
                elif isinstance(obj, str):
                    refs.append(by)
                    ids.append(Sym(obj))
                else:
                    raise TypeError("can't infer path-id for %s of %s (%s)"
                                    % (by, type(self).__name__, type(obj).__name__))
        return IdPath(tuple(refs), tuple(ids))

    def __str__(self):
        return str(self.id)

    def __repr__(self):
        return repr(self.id)

    def __hash__(self):
        return hash(self.id.idd)

    def __eq__(self, other):
        if isinstance(other, Idd): 
            return self.id.idd == other.id.idd
        else:
            return NotImplemented


