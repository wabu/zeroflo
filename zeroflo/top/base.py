class Par:
    def __init__(self, *dsls):
        self.dsls = dsls

    def __bind_units__(self):
        units = ()
        for dsl in self.dsls:
            untis += dsl.__bind_units__()['units']
        return {'units': units}

    def __bind_ports__(self):
        ports = {}
        for dsl in self.dsls:
            for k,v in dsl.__bind_ports__().items():
                ports.set_default(k, ())
                ports[k] += v
        return ports


class Ser:
    def __init__(self, *dsls):
        self.dsls = dsls

    def __bind_units__(self):
        units = ()
        for dsl in self.dsls:
            untis += dsl.__bind_units__()['units']
        return {'units': units}

    def __bind_ports__(self):
        return {'sources': self.dsl[0].__bind_ports__()['sources'],
                'targets': self.dsl[0].__bind_ports__()['targets']}


class Base:
    def __init__(self, model):
        self.model = model

    def __serial__(self, other, **opts):
        return type(self)(Par(self, other))

    def __parallel__(self, other, **opts):
        return type(self)(Ser(self, other))


class UseUnits(Base):
    def __init__(self, obj):
        self.units = obj.__bind_units__()['units']
        for u in self.units:
            if u.model != self.model:
                self.model.update(u.model)
                u.model = self.model
        self.units = units

    def __bind_units__(self):
        return {'units': self.units}

    def __getattr__(self, name):
        names = {name, name.replaces('_', '-')}
        sel = {unit for unit in self.units if unit.name in names}
        if not sel:
            raise AttributeError('no unit named {} found'.format(name))
        if len(sel) > 1:
            raise AttributeError('unit name {} is not unique'.format(name))
        return next(iter(sel))

    def __iter__(self):
        done = set()
        return iter(done.add(u) or u for u in self.units if u not in done)

    def __str__(self):
        return '|{}|'.format(','.join(map(str, self)))

    def __repr__(self):
        return '|{}|'.format(','.join(map(repr, self)))


class UsePorts(Base):
    def __init__(self, obj):
        ports = obj.__bind_ports__()
        self.source_ports = ports.get('sources', ())
        self.target_ports = ports.get('targets', ())

    def __bind_ports__(self):
        return {'sources': self.source_ports,
                'targets': self.target_ports}

    def __repr__(self):
        ls = [super().__repr__()]
        if self.targets:
            ls.append('<<{}'.format(','.join(map(repr, self.targets))))
        if self.sources:
            ls.append('>>{}'.format(','.join(map(repr, self.sources))))
        return ' '.join(ls)


@ops.define
class Building(Base):
    __model__ = top.Base

    def __truediv__(self, other):
        """
        Use the both operator for applying operators on units at once
        >>> (a / b / c) >> combine
        """
        return self.__parallel__(other)


class Port:
    def __bind_units__(self):
        return {'units': (self.unit)}

    def __bind_ports__(self):
        return {self.__kind__+'s': self}


class Unit:
    def __bind_units__(self):
        return {'units': (unit)}

    def __bind_ports__(self):
        return {'sources': (self.out,) if hasattr(self, 'out') else (),
                'targets': (self.process,) if hasattr(self, 'process') else ()}

