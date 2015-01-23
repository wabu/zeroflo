from pyadds.meta import ops


class ModelBase:
    """
    ModelBase it the base for cooperative model components of the flow.

    The models constructer may consume some options and they must support
    updates, incooperating another model into themselfs.
    """
    def update(self, other):
        pass


class Builds:
    """
    This is a base class for components that interact with the model.
    Different builder componets manage different aspects of the flow
    model and when combined they are used to buildup the model.

    Build components are based on other objects, which export `__bind_...__`
    methods used to access the aspacts of the object the modelers is interrested
    in. For example, a unit and it's ports are such objects, on which builders
    that link ports or distribute units are based appon. Moreover a builder
    can also be based on other builders or combinations thereof.

    :see: zeroflo.model.links, zeroflo.model.spaces
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


class RequireUnits(Builds):
    """
    Ensures that the build component has units to work on.
    """
    def __init__(self, on):
        super().__init__(on)
        self.units = on.__bind_units__()


class RequirePorts(Builds):
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


class Combines(Builds):
    """
    Combine this build component with another into a single one, operating
    on both of the original components.
    """
    @ops.opsame
    def __truediv__(self, other: Builds):
        return self.__combine__(other)
