import weakref

from pyadds.meta import ops


class Referrer:
    """
    Multiple referrer objects refer to referred object, so that the referred
    instance can be replaced by an updated one for all referrers

    The public interface of the referred instance is also available on the
    referrer object.
    """
    def __init__(self, model):
        self.instance = model
        model.refer(self)

    def __getattr__(self, item):
        """ forward requests to the referred instance """
        return getattr(self.instance, item)

    def __eq__(self, other):
        """ check for referred objects equality """
        if isinstance(other, Referrer):
            return self.instance == other.instance
        elif isinstance(other, ModelBase):
            return self.instance == other
        else:
            return False

    def __hash__(self):
        return hash(self.instance)

    def __str__(self):
        return '*{}'.format(self.instance)

    def __repr__(self):
        return '*{!r}'.format(self.instance)


class Referred:
    """
    object being referred indirectly so the referred instance can be replaced.
    """
    def __init__(self):
        self._refers = weakref.WeakSet()

    def refer(self, ref):
        """
        add a referrer to this instance
        """
        ref.instance = self
        self._refers.add(ref)

    def replaces(self, other):
        """
        Let this instance replace another instance for all references to the
        other instance
        :param other: another Referred instance
        :returns self:
        """
        assert isinstance(other, Referred)
        for ref in other._refers:
            self.refer(ref)
        return self


class ModelBase(Referred):
    """
    ModelBase it the base for cooperative model components of the flow.

    Model components must support `unify`, and may hook into the
    `register`/`unregistered` methods
    """

    def unify(self, other):
        """
        called to unify with another model components, updating itself with the
        content of the other model
        :param other: other model instance to unify with
        :returns None:
        """
        self.replaces(other)

    def register(self, *units):
        """
        called to register units within this model
        """
        for u in units:
            if u.model != self:
                self.unify(u.model.instance)

    def unregister(self, unit):
        """
        called to unregister units within this model
        """
        del unit.model


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
        self.on = on

        assert isinstance(on.model, Referrer)
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
        """ leftmost of the units """
        return self.units[0]

    @property
    def right(self):
        """ rightmost of the units """
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
                model.unify(base.model.instance)
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
