from collections import Counter

from pyadds.ops import opsame, autowraped_ops
from pyadds.str import name_of


class Tp:
    def __getattr__(self, name):
        def printer(*args, **kws):
            pts = ([repr(a) for a in args] +
                   ['{}={}'.format(k, v) for k, v in kws.items()])
            print('tp.{}({})'.format(name, ', '.join(pts)))
        return printer


def withtp(f):
    tp = Tp()

    def add_tp(*args, **kws):
        return f(*args, tp=tp, **kws)
    return add_tp


def flatten(its):
    return [i for it in its for i in it]


class UnitsDSL:
    __slots__ = ('units', 'sources', 'targets')

    def __init__(self, units, sources=None, targets=None):
        self.units = tuple(units)
        if sources is None:
            sources = [u.out for u in units if hasattr(u, 'out')]
        if targets is None:
            targets = [u.process for u in units if hasattr(u, 'process')]
        self.sources = sources
        self.targets = targets

    @classmethod
    def join(cls, items):
        return cls(flatten(i.dsl.units for i in items),
                   sources=flatten(i.dsl.sources for i in items),
                   targets=flatten(i.dsl.targets for i in items))

    @property
    def dsl(self):
        return self

    @property
    def left(self):
        return self.units[0]

    @property
    def right(self):
        return self.units[-1]

    @opsame
    def __add__(self, other):
        return UnitsDSL(self.units + other.units,
                        sources=other.sources,
                        targets=self.targets)

    @opsame
    @withtp
    def __and__(self, other, tp):
        tp.join(self.right, other.left)
        return self + other

    @opsame
    @withtp
    def __or__(self, other, tp):
        tp.part(self.right, other.left)
        return self + other

    @withtp
    def __mul__(self, num, tp):
        tp.bundle(self.units, mul=num)
        return self

    @withtp
    def __pow__(self, num, tp):
        tp.bundle(self.units, repl=num)
        return self

    @withtp
    def __xor__(self, key, tp):
        tp.bundle(self.units, map=key)
        return self

    @withtp
    def __rshift__(self, other, tp):
        if isinstance(other, tuple):
            return self >> UnitsDSL.join(other)
        elif isinstance(other, UnitsDSL):
            for src in self.sources:
                for tgt in other.targets:
                    tp.link(src, tgt)
            return self + other
        else:
            return NotImplemented

    def __rrshift__(self, other):
        if isinstance(other, tuple):
            return UnitsDSL.join(other) >> self
        else:
            return NotImplemented

    def __iter__(self):
        done = set()
        return iter(done.add(u) or u for u in self.units if u not in done)

    def __str__(self):
        return '|{}|'.format(','.join(map(str, self)))

    def __repr__(self):
        ls = []
        ls.append('|{}|'.format(','.join(map(repr, self))))
        if self.targets:
            ls.append('>>{}'.format(self.targets))
        if self.sources:
            ls.append('{}>>'.format(self.sources))
        return ' '.join(ls)


DSLMixin = autowraped_ops(UnitsDSL, by='dsl')


class NamedMixin:
    __next_idz = Counter()

    def __init__(self, *args, **kws):
        self.name = name = kws.pop('name', None) or name_of(self)
        self.__next_idz.update([name])
        self.idz = self.__next_idz[name]

    def __str__(self):
        return self.name

    def __repr__(self):
        return '{}@{:04x}'.format(self.name, self.idz)
