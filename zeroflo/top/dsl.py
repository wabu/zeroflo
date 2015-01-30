from pyadds.meta import ops
from pyadds.annotate import refers

from .topology import Topology


class FloDSL:
    def __init__(self, units, *, topology, sources=None, targets=None):
        self.topology = topology
        for u in units:
            if u.topology != topology:
                topology.update(u.topology)
                u.topology = topology

        self.units = tuple(units)
        if sources is None:
            sources = [u.out for u in units if hasattr(u, 'out')]
        if targets is None:
            targets = [u.process for u in units if hasattr(u, 'process')]
        self.sources = sources
        self.targets = targets

    @property
    def left(self):
        return self.units[0]

    @property
    def right(self):
        return self.units[-1]

    def __getattr__(self, name):
        names = {name, name.replaces('_', '-')}
        sel = {unit for unit in self.units if unit.name in names}
        if not sel:
            raise AttributeError('no unit named {} found'.format(name))
        if len(sel) > 1:
            raise AttributeError('unit name {} is not unique'.format(name))
        return next(iter(sel))

    @ops.opsame
    def __add__(self, other):
        return type(self)(self.units + other.units,
                          targets=self.targets,
                          sources=other.sources,
                          topology=self.topology)

    @ops.opsame
    def __truediv__(self, other):
        return type(self)(self.units + other.units,
                          targets=self.targets + other.targets,
                          sources=self.sources + other.sources,
                          topology=self.topology)

    @ops.opsame
    def __and__(self, other):
        self.topology.join(self.right, other.left)
        return self + other

    @ops.opsame
    def __or__(self, other):
        self.topology.par(self.right, other.left)
        return self + other

    def __mul__(self, num):
        self.topology.bundle(self.units, mul=num)
        return self

    def __pow__(self, num):
        self.topology.bundle(self.units, repl=num)
        return self

    def __xor__(self, key):
        self.topology.bundle(self.units, map=key)
        return self

    @ops.opsame
    def __rshift__(self, other):
        for src in self.sources:
            for tgt in other.targets:
                self.topology.link(src, tgt)
        return self + other

    def __iter__(self):
        done = set()
        return iter(done.add(u) or u for u in self.units if u not in done)

    def __str__(self):
        return '|{}|'.format(','.join(map(str, self)))

    def __repr__(self):
        ls = []
        ls.append('|{}|'.format(','.join(map(repr, self))))
        if self.targets:
            ls.append('<<{}'.format(','.join(map(repr, self.targets))))
        if self.sources:
            ls.append('>>{}'.format(','.join(map(repr, self.sources))))
        return ' '.join(ls)


DSLMixin = ops.autowraped_ops(FloDSL, by='dsl')


class UnitDSL(DSLMixin):
    @refers
    def topology(self):
        top = Topology()
        top.register(self)
        return top

    @property
    def dsl(self):
        return FloDSL(units=(self,), topology=self.topology)


class SourceDSL(DSLMixin):
    @property
    def dsl(self):
        return FloDSL(units=(self.unit,), sources={self}, targets={},
                      topology=self.unit.topology)


class TargetDSL(DSLMixin):
    @property
    def dsl(self):
        return FloDSL(units=(self.unit,), sources={}, targets={self},
                      topology=self.unit.topology)
