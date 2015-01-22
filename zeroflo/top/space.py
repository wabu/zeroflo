class Space:
    def __init__(self, units=None):
        self.units = units or set()

    def __str__(self):
        return '&'.join(map(str, self.units))

    def __repr__(self):
        return '({})'.format(' & '.join(map(repr, self.units)))


class Spaces(Units):
    def __init__(self):
        super().__init__()
        # {unit: space}
        self._spaces = {}
        # {space: {space}}
        self._pars = defaultdict(set)

    @property
    def spaces(self):
        return set(self._spaces.values())

    def space_of(self, unit):
        return self._spaces.get(unit)

    def join(self, *units):
        # spaces that may get joined
        spaces = set(filter(None, map(self.space_of, units)))
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
            space = self.space_of(unit)
            if space:
                if len(space.units.intersection(units)) > 1:
                    raise IndexError('Cannot par joined units')

        spaces = {self.space_of(unit) or self.join(u) for u in units}
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


class Spacing(WithUnits):
    __model__ = top.Spaces

    @ops.opsame
    def __and__(self, other):
        """ Joins components spaces """
        for a in self.units:
            for b in other.units:
                self.model.join(a, b)
        return self.__parallel__(other)

    @ops.opsame
    def __or__(self, other):
        """ parrallizes components spaces """
        for a in self.units:
            for b in other.units:
                self.model.par(a, b)
        return self.__parallel__(other)
