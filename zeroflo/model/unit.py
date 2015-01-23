from pyadds.annotate import Conotate, cached
from pyadds.str import name_of

from zeroflo.model import Builds


class Unit(Builds):
    def __init__(self, name=None):
        super().__init__()
        self.name = name_of(self)

    def __bind_units__(self):
        return [self]

    def __bind_ports__(self):
        mapping = [('sources', 'out'),
                   ('targets', 'process')]
        return {key: [getattr(self, attr)]
                for key, attr in mapping if hasattr(self, attr)}

    def __process__(self):
        while True:
            port, load, tag = yield from recv(self)
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
        self.defininition = definition

    def __bind_units__(self):
        return [self.unit]

    def __bind_ports__(self):
        return {'{}s'.format(self.__kind__): [self]}

    def __str__(self):
        return '{}.{}'.format(self.unit, self.name)

    __repr__ = __str__


class TargetPort(Port):
    __kind__ = 'target'


class SourcePort(Port):
    __kind__ = 'source'


class port(Conotate, cached):
    """
    decorateor base to define ports for a unit
    >>> class Foo(Unit):
        @inport
        def bar(self, data, tag):
            ...
    """
    __port__ = Port

    def __default__(self, unit):
        return self.__port__(unit, self.name, self.definition)


class inport(port):
    __port__ = TargetPort


class outport(port):
    __port__ = SourcePort
