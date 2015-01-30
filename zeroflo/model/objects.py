import asyncio

from pyadds.annotate import Conotate, refers
from pyadds.str import name_of

from zeroflo.model import Builds
from zeroflo import context


class Unit(Builds):
    def __init__(self, name=None):
        super().__init__()
        self.name = name or name_of(self)

    def __bind_units__(self):
        return [self]

    def __bind_ports__(self):
        mapping = [('sources', 'out'),
                   ('targets', 'process')]
        return {key: [getattr(self, attr)]
                for key, attr in mapping if hasattr(self, attr)}

    def __process__(self):
        while True:
            port, load, tag = yield from self.recv(self)
            if port is None:
                break
            yield from port(load, tag)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '{}-{:x}'.format(self, id(self))


class Port(Builds):
    __kind__ = None

    def __init__(self, unit, name, definition):
        self.unit = unit
        self.name = name
        self.definition = definition

    @property
    def model(self):
        return self.unit.model

    def __bind_units__(self):
        return [self.unit]

    def __bind_ports__(self):
        return {'{}s'.format(self.__kind__): [self]}

    def __call__(self, *args, **kws):
        if asyncio.get_event_loop().is_running():
            return self.definition(*args, **kws)
        else:
            # XXX implement this
            context.get_control(self.unit.model).run(self, *args, **kws)

    def __str__(self):
        return '{}.{}'.format(self.unit, self.name)

    __repr__ = __str__


class TargetPort(Port):
    __kind__ = 'target'


class SourcePort(Port):
    __kind__ = 'source'


class PortAnnotation(Conotate, refers):
    """
    decorator base to define ports for a unit
    """
    __port__ = Port

    def __default__(self, unit):
        return self.__port__(unit, self.name, self.definition)


class InPortAnnotation(PortAnnotation):
    __port__ = TargetPort


class OutPortAnnotation(PortAnnotation):
    __port__ = SourcePort


inport = InPortAnnotation
outport = OutPortAnnotation
