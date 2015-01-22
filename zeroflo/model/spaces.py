from pyadds.meta import ops

from zeroflo.model.base import HasUnits, Model


class Spaces(Model):
    def __init__(self):
        super().__init__()
        self._spaces = []

    def spaces(self, units=None):
        spaces = []
        for space in self._spaces:
            if units is None or set(space.units).intersection(units):
                spaces.append(space)
        return spaces

    def join(self, *other):
        ...

    def par(self, *other):
        ...


class MaySpace(HasUnits):
    @ops.opsame
    def __and__(self, other: HasUnits):
        res = self.__combine__(other)
        self.model.join(other)
        return res

    @ops.opsame
    def __or__(self, other: HasUnits):
        res = self.__combine__(other)
        self.model.par(other)
        return res
