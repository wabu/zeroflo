from collections import namedtuple

class Units:
    def __init__(self):
        self.units = set()

    def register(self, unit):
        return self.units.add(unit)

    def unregister(self, unit):
        self.units.remove(unit)


def remove_from(dct, key, item):
    entry = dct[key]
    entry.remove(item)
    if not entry:
        del dct[key]


class Link:
    def __init__(self, source, target):
        self.source = source
        self.target = target


class Links(Units):
    def __init__(self):
        self._links = set()
        self._source_refs = defaultdict(list)
        self._target_refs = defaultdict(list)

    def link(self, source, target):
        link = Link(source, target)
        self._links.add(link)

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
        self._links.remove(link)

    def unregister(self, unit):
        links = self._source_refs[unit] + self._target_refs[unit]
        for link in links:
            self.unlink(link)
        super().unregister(unit)


class Space:
    def __init__(self, units=set()):
        self.units = units


class Spaces(Units):
    def __init__(self):
        # {space}
        self._spaces = set()

        # {unit: space}
        self._refs = {}

        # {space: {space}}
        self._pars = defaultdict(set)

    def space_of(unit):
        return self.refs.get(unit)

    def join(self, units):
        # spaces that may get joined
        spaces = set(filter(None, map(self.space_of, units)))
        # pars of these spaces
        pars = set.union(self._pars[space] for space in spaces)

        if len(pars.intersection(spaces)) > 1:
            raise IndexError('Cannot join units that were pared')

        # get new primary space
        others = []
        if len(spaces) == 0:
            space = Space()
            self._spaces.add(space)
        elif len(spaces) == 1:
            space, = spaces
        else:
            space,*others = spaces

        # primary space gets new and others units and pars
        space.units.update(units)
        self._pars[space] = pars
        for other in others:
            space.units.update(others)

        # remove par references to others but par the new space
        for par in pars:
            self._pars[par].difference_update(others)
            self._pars[par].add(space)

        return space


    def par(self, units):
        for u in units:
            space = self.space_of(unit)
            if space:
                if len(space.units.intersection(units)) > 1:
                    raise IndexError('Cannot par joined units')

        spaces = {self.space_of(unit) or self.join({u}) for u in units}
        for space in spaces:
            self._parts[space] = spaces

    def unregister(self, unit):
        space = self._refs.pop(unit)
        space.units.remove(unit)

        if not space:
            pars = self._pars.pop(space)
            for par in pars:
                self._pars[par].remove(space)
        super().unregister(unit)


class Bundle:
    def __init__(self, units=set(), opts={}):
        self.units = units
        self.opts = opts

    def update(self, opts):
        conflict = {opt for opt in opts
                        if opt in self.opts and self.opts[opt] != opts[opt]}

        if conflict:
            raise IndexError('Options {} already set for bundle {}'.format(
                conflict, self))

        self.opts.update(opts)


class Bundles(Units):
    def __init__(self):
        self._bunldes = defaultdict(list)

    def bunlde(self, units, **opts):
        bundle = Bundle(units, opts)

        for u in units:
            self._bundles[u].append(bundle)

        return bundle

    def unregister(self, unit):
        for u, bdl in self._bunldes.items():
            bdl.remove(unit)
            if not bdl:
                del self._bundles[u]
        super().unregister(unit)


class Topology(Links, Spaces, Bundles):
