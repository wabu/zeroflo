from pyadds.meta import ops


class ModelBase:
    def __init__(self):
        self._refers = set()

    def __refer__(self, ref):
        self._refers.add(ref)
        ref.instance = self

    def __update__(self, other):
        assert isinstance(other, ModelBase)
        for ref in other._refers:
            self.__refer__(ref)

    def unregister(self, unit):
        pass


class ModelRef:
    def __init__(self, model):
        self.instance = model
        model.__refer__(self)

    def __getattr__(self, item):
        return getattr(self.instance, item)

    def __eq__(self, other):
        if isinstance(other, ModelRef):
            return self.instance is other.instance
        elif isinstance(other, ModelBase):
            return self.instance is other
        else:
            return False

    def __hash__(self):
        return hash(self.instance)

    def __str__(self):
        return '*{}'.format(self.instance)

    def __repr__(self):
        return '*{!r}'.format(self.instance)


class Units(ModelBase):
    """
    ModelBase it the base for cooperative model components of the flow.

    The models constructor may consume some options and they must support
    updates, incorporating another model into themselves.
    """
    def __init__(self):
        super().__init__()
        self._units = []

    def units(self):
        return self._units

    def __update__(self, other):
        assert isinstance(other, Units)
        super().__update__(other)
        self._units.extend(other._units)

    def register(self, *units):
        for u in units:
            if u.model != self:
                self.__update__(u.model.instance)
            else:
                self._units.append(u)

    def unregister(self, unit):
        super().unregister(unit)
        self._units.remove(unit)

    def __str__(self):
        return '|{}|'.format(','.join(map(str, self._units)))

    def __repr__(self):
        return '|{:x}:{}|'.format(id(self),
                                  ','.join(map(str, self._units)))


class Builder:
    """
    This is a base class for components that interact with the model.
    Different builder components manage different aspects of the flow
    model and when combined they are used to buildup the model.

    Build components are based on other objects, which export `__bind_...__`
    methods used to access the aspects of the object the modelers is interested
    in. For example, a unit and it's ports are such objects, on which builders
    that link ports or distribute units are based upon. Moreover a builder
    can also be based on other builders or combinations thereof.
    """
    def __init__(self, on):
        assert isinstance(on.model, ModelRef)
        self.on = on
        self.model = on.model

    def __combine__(*objs):
        self = objs[0]
        return type(self)(Combine(self.model, *[o.on for o in objs]))

    def __series__(*objs):
        self = objs[0]
        return type(self)(Series(self.model, *[o.on for o in objs]))


class RequireUnits(Builder):
    """
    Ensures that the build component has units to work on.
    """
    def __init__(self, on):
        super().__init__(on)
        self.units = on.__bind_units__()

    @property
    def left(self):
        return self.units[0]

    @property
    def right(self):
        return self.units[-1]


class RequirePorts(Builder):
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
        self._model = None
        self.model = model

    @property
    def model(self):
        return self._model

    @model.setter
    def model(self, model):
        for base in self.bases:
            if base.model != model:
                model.__update__(base.model.instance)
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
        return {'targets': self.bases[0].__bind_ports__().get('targets', []),
                'sources': self.bases[-1].__bind_ports__().get('sources', [])}


class Combines(Builder):
    """
    Combine this build component with another into a single one, operating
    on both of the original components.
    """
    @ops.opsame
    def __truediv__(self, other: Builder):
        return self.__combine__(other)
