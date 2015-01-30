from collections import defaultdict
from pyadds.meta import ops
from pyadds import Anything

from zeroflo.model.base import RequireUnits, ModelBase


class Space:
    """
    information about a space for units
    """
    def __init__(self, units=None):
        self.units = units or set()

    def __str__(self):
        return '&'.join(map(str, self.units))

    def __repr__(self):
        return '({})'.format(' & '.join(map(repr, self.units)))


class Spaces(ModelBase):
    """
    models the spacing of units across different processes
    """
    def __init__(self):
        super().__init__()
        # {unit: space}
        self._spaces = []
        # {space: {space}}
        self._pars = defaultdict(set)

    def spaces(self, units=Anything):
        if not isinstance(units, set):
            units = {units}
        spaces = []
        for space in self._spaces:
            if units.intersection(set(space.units)):
                spaces.append(space)
        return spaces

    def join(self, *units):
        spaces = self.spaces(set(units))
        # pars of these spaces
        pars = set.union(*(self._pars[space] for space in spaces)) if spaces else set()
        if len(pars.intersection(spaces)) > 1:
            raise IndexError('Cannot join units that were pared')

        # get new primary space
        others = []
        if len(spaces) == 0:
            space = Space()
            self._spaces.append(space)
        elif len(spaces) == 1:
            space, = spaces
        else:
            space, *others = spaces

        # primary space gets new and others units and pars
        space.units.update(units)

        self._pars[space] = pars
        for other in others:
            space.units.update(other.units)
            self._spaces.remove(other)

        # remove par references to others but par the new space
        for par in pars:
            self._pars[par].difference_update(others)
            self._pars[par].add(space)

        return space

    def par(self, *units):
        spaces = set(self.spaces(set(units)))
        spaced = set()
        for space in spaces:
            if len(space.units.intersection(units)) > 1:
                raise IndexError('Cannot par joined units')
            spaced.update(space.units)

        new = set(units).difference(spaced)
        spaces.update(map(self.join, new))

        for space in spaces:
            self._pars[space] = spaces.difference({space})

    def unregister(self, unit):
        super().unregister(unit)

        spaces = self.spaces(unit)
        if not spaces:
            return
        space = spaces[0]
        space.units.remove(unit)
        if not space.units:
            pars = self._pars.pop(space)
            for par in pars:
                if par.units:
                    self._pars[par].remove(space)
            spaces.remove(space)

    def unify(self, other):
        assert isinstance(other, Spaces)
        super().unify(other)
        self._spaces.extend(other._spaces)
        self._pars.update(other._pars)


class BuildSpaces(RequireUnits):
    """
    builds spacings for units using `a & b | c & d` syntax
    """
    @ops.opsame
    def __and__(self, other: RequireUnits):
        res = self.__combine__(other)
        self.model.join(self.right, other.left)
        return res

    @ops.opsame
    def __or__(self, other: RequireUnits):
        res = self.__combine__(other)
        self.model.par(self.right, other.left)
        return res
