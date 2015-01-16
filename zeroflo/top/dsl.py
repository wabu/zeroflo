from pyadds.meta import ops


class FloDSL:
    def __init__(self, units, sources=None, targets=None, model=None):
        self.model = model
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
        names = {name, name.replace('_', '-')}
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
                          model=self.model)

    @ops.opsame
    def __truediv__(self, other):
        return type(self)(self.units + other.units,
                          targets=self.targets + other.targets,
                          sources=self.sources + other.sources,
                          model=self.model)

    @ops.opsame
    def __and__(self, other):
        self.model.join(self.right, other.left)
        return self + other

    @ops.opsame
    def __or__(self, other):
        self.model.par(self.right, other.left)
        return self + other

    def __mul__(self, num):
        self.model.bundle(self.units, mul=num)
        return self

    def __pow__(self, num):
        self.model.bundle(self.units, repl=num)
        return self

    def __xor__(self, key):
        self.model.bundle(self.units, map=key)
        return self

    @ops.opsame
    def __rshift__(self, other):
        for src in self.sources:
            for tgt in other.targets:
                self.model.link(src, tgt)
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
    def __init__(self, model):
        self.model = model

    @property
    def dsl(self):
        return FloDSL(units=(self,), model=self.model)


class SourceDSL(DSLMixin):
    def __init__(self, model):
        self.model = model

    @property
    def dsl(self):
        return FloDSL(units=(self.unit,), sources={self}, targets={},
                      model=self.model)


class TargetDSL(DSLMixin):
    def __init__(self, model):
        self.model = model

    @property
    def dsl(self):
        return FloDSL(units=(self.unit,), sources={}, targets={self},
                      model=self.model)
