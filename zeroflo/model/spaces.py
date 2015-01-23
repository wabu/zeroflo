from collections import defaultdict
from pyadds.meta import ops

from zeroflo.model.base import RequireUnits, ModelBase


class Space:
    def __init__(self, units=None):
        self.units = units or set()

    def __str__(self):
        return '&'.join(map(str, self.units))

    def __repr__(self):
        return '({})'.format(' & '.join(map(repr, self.units)))


class Spaces(ModelBase):
    def __init__(self):
        super().__init__()
        # {unit: space}
        self._spaces = {}
        # {space: {space}}
        self._pars = defaultdict(set)

    def spaces(self, units=None):
        spaces = []
        if units is not None and not isinstance(units, set):
            units = {units}
        for space in self._spaces.values():
            if units is None or set(space.units).intersection(units):
                spaces.append(space)
        return spaces

    def join(self, *units):
        # spaces that may get joined
        spaces = set(filter(None, map(self.spaces, units)))
        # pars of these spaces
        if spaces:
            pars = set.union(*(self._pars[space] for space in spaces))
        else:
            pars = set()

        if len(pars.intersection(spaces)) > 1:
            raise IndexError('Cannot join units that were pared')

        # get new primary space
        others = []
        if len(spaces) == 0:
            space = Space()
        elif len(spaces) == 1:
            space, = spaces
        else:
            space, *others = spaces

        # primary space gets new and others units and pars
        space.units.update(units)
        for u in units:
            self._spaces[u] = space

        self._pars[space] = pars
        for other in others:
            space.units.update(other.units)
            for u in other.units:
                self._spaces[u] = space

        # remove par references to others but par the new space
        for par in pars:
            self._pars[par].difference_update(others)
            self._pars[par].add(space)

        return space

    def par(self, *units):
        for unit in units:
            space = self.spaces(unit)
            if space:
                if len(space.units.intersection(units)) > 1:
                    raise IndexError('Cannot par joined units')

        spaces = {self.spaces(unit) or self.join(u) for u in units}
        for space in spaces:
            self._pars[space] = spaces

    def unregister(self, unit):
        super().unregister(unit)

        if unit not in self._spaces:
            return

        space = self._spaces.pop(unit)
        space.units.remove(unit)

        if not space.units:
            pars = self._pars.pop(space)
            for par in pars:
                if par.units:
                    self._pars[par].remove(space)

    def update(self, other):
        super().update(other)
        self._spaces.update(other._spaces)
        self._pars.update(other._pars)


class BuildSpaces(RequireUnits):
    @ops.opsame
    def __and__(self, other: RequireUnits):
        res = self.__combine__(other)
        self.model.join(other)
        return res

    @ops.opsame
    def __or__(self, other: RequireUnits):
        res = self.__combine__(other)
        self.model.par(other)
        return res
