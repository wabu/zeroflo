from pyadds.meta import ops

from zeroflo import context


class Model:
    def update(self, other):
        pass


class Build:
    """
    This is a base class for build components, which are used to build-up the
    flow model.  Different build componets manage different aspects of the flow
    model and are combined to build the model.

    Build components are based on other objects, which export `__bind_...__`
    methods used to access the aspacts of the object the builder is interrested
    in. For example, a unit and it's ports are such objects, on which builders
    that link ports or distribute units are based appon. Moreover a builder
    can also be based on other builders or combinations thereof.
    """
    def __init__(self, on):
        self.on = on
        self.model = on.model

    def __combine__(*objs):
        self = objs[0]
        return type(self)(Combine(self.model, *[o.on for o in objs]))

    def __series__(*objs):
        self = objs[0]
        return type(self)(Series(self.model, *[o.on for o in objs]))


class HasUnits(Build):
    """
    Ensures that the build component has units to work on.
    """
    def __init__(self, on):
        super().__init__(on)
        self.units = on.__bind_units__()


class HasPorts(Build):
    """
    Ensures that the build component has units to work on.
    """
    def __init__(self, on):
        super().__init__(on)
        self.ports = on.__bind_ports__()
        self.source_ports = self.ports.get('sources') or []
        self.target_ports = self.ports.get('targets') or []


class Combine:
    """
    Combines multiple build components into a single one,
    exporting all there units and ports.
    """
    def __init__(self, model, *bases):
        self.bases = bases
        self.model = model

    @property
    def model(self):
        return self._model

    @model.setter
    def model(self, model):
        for base in self.bases:
            if base.model != model:
                model.update(base.model)
                base.model = model
        self._model = model

    def __bind_units__(self):
        units = []
        for base in self.bases:
            units.extend(base.__bind_units__())
        return units

    def __bind_ports__(self):
        ports = {}
        for base in self.bases:
            for k, ps in base.__bind_ports__().items():
                ports.setdefault(k, []).extend(ps)
        return ports


class Series(Combine):
    """
    Combines a series of build components into a single one,
    exporting all there units, but only sources of the first and targets of the
    last unit.
    """
    def __bind_ports__(self):
        return {'targets': self.bases[0].__bind_ports__()['targets'],
                'sources': self.bases[-1].__bind_ports__()['sources']}


class MayCombine(Build):
    """
    Tries to combine this build component with others
    """
    @ops.opsame
    def __truediv__(self, other: Build):
        return self.__combine__(other)
