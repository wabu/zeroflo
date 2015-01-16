from collections import defaultdict


class Units:
    def __init__(self):
        self.units = set()

    def register(self, unit):
        return self.units.add(unit)

    def unregister(self, unit):
        self.units.remove(unit)

    def update(self, other):
        self.units.update(other.units)


def remove_from(dct, key, item):
    entry = dct[key]
    entry.remove(item)
    if not entry:
        del dct[key]


class Link:
    def __init__(self, source, target):
        self.source = source
        self.target = target

    def __str__(self):
        return '{}>>{}'.format(self.source, self.target)

    def __repr__(self):
        return '{!r} >> {!r}'.format(self.source, self.target)


class Links(Units):
    def __init__(self):
        super().__init__()
        # {unit: {link}}
        self._source_refs = defaultdict(set)
        self._target_refs = defaultdict(set)

    @property
    def links(self):
        return set.union(*self._source_refs.values() or [set()])

    def link(self, source, target):
        link = Link(source, target)

        self._source_refs[source.unit].add(link)
        self._target_refs[target.unit].add(link)
        return link

    def links_from(self, source):
        try:
            source = source.unit
        except AttributeError:
            pass
        return self._source_refs[source]

    def links_to(self, target):
        try:
            target = target.unit
        except AttributeError:
            pass
        return self._target_refs[target]

    def unlink(self, link):
        remove_from(self._source_refs, link.source.unit, link)
        remove_from(self._target_refs, link.target.unit, link)

    def unregister(self, unit):
        links = self._source_refs[unit] | self._target_refs[unit]
        for link in links:
            self.unlink(link)
        super().unregister(unit)

    def update(self, other):
        super().update(other)
        self._source_refs.update(other._source_refs)
        self._target_refs.update(other._target_refs)


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


class Bundle:
    def __init__(self, units=set(), opts={}):
        self.units = units
        self.opts = opts


class Bundles(Units):
    def __init__(self):
        super().__init__()
        # {unit: {bundle}}
        self._bundles = defaultdict(set)

    @property
    def bundles(self):
        return set.union(*self._bundles.values())

    def bundles_of(self, *units):
        return set.union(*(self._bundles[u] for u in units))

    def bundle(self, units, **opts):
        bundle = Bundle(units, opts)

        for u in units:
            self._bundles[u].add(bundle)

        return bundle

    def unregister(self, unit):
        for bdl in self._bundles.pop(unit, {}):
            bdl.units.remove(unit)
        super().unregister(unit)

    def update(self, other):
        super().update(other)
        self._bundles.update(other._bundles)


class Topology(Links, Spaces, Bundles):
    pass
