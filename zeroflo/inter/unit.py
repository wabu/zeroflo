from collections import Counter
from asyncio import coroutine

from pyadds.str import name_of
from pyadds.annotate import Conotate, RefDescr, Get

from .dsl import DSLMixin, UnitsDSL


class NamedMixin:
    __next_idz = Counter()

    def __init__(self, *args, **kws):
        self.name = name = kws.pop('name', None) or name_of(self)
        self.__next_idz.update([name])
        self.idz = self.__next_idz[name]

    def __str__(self):
        return self.name

    def __repr__(self):
        return '{}-{:04x}'.format(self.name, self.idz)


class Unit(NamedMixin, DSLMixin):
    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)

    @property
    def dsl(self):
        return UnitsDSL([self])

    @coroutine
    def __setup__(self):
        pass

    @coroutine
    def __close__(self):
        pass


class Port(DSLMixin):
    def __init__(self, unit, portdef):
        self.unit = unit
        self.portdef = portdef

    @property
    def dsl(self):
        return UnitsDSL([self.unit], **{self.__kind__, self})

    def __str__(self):
        return '{}.{}'.format(self.unit, self.portdef.name)

    def __repr__(self):
        return '{!r}.{}'.format(self.unit, self.portdef.name)


class OutPort(Port):
    __kind__ = 'source'


class InPort(Port):
    __kind__ = 'target'


class PortDef(Conotate, RefDescr, Get):
    __port__ = None

    def __default__(self, unit):
        return self.__port__(unit, self)


class inport(PortDef):
    __port__ = InPort


class outport(PortDef):
    __port__ = OutPort
