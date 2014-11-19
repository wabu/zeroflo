from pyadds.meta import ops
from pyadds import annotate as an


def flatten(its):
    return (i for it in its for i in it)


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
        return self.units[0]

    @classmethod
    def join(cls, units):
        return cls(flatten(i.dsl.units for i in items),
                   sources=flatten(i.dsl.sources for i in items),
                   targets=flatten(i.dsl.targets for i in items))

    
    def __add__(self, other):
        return type(self)(self.units + other.units,
                          targets=self.targets,
                          sources=other.sources)

    def __and__(self, other):
        self.model.join(self.right, other.left)
        return self + other

    def __or__(self, other):
        self.model.part(self.right, other.left)
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

    def __rshift__(self, other):
        if isinstance(other, tuple):
            return self >> self.join(other)
        elif isinstance(other, FloDSL):
            for src in self.sources:
                for tgt in other.targets:
                    self.model.link(src, tgt)
            return self + other
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
            ls.append('>>{}'.format(self.sources))
        return ' '.join(ls)


DSLMixin = ops.autowraped_ops(FloDSL, by='dsl')
