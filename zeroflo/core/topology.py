from collections import defaultdict

from pyadds.annotate import delayed

from .idd import Idd, Id

import os

class Unit(Idd):
    """ topology info of a unit """

    __by__ = 'space'

    def __init__(self, space, hints=None, **kws):
        super().__init__(**kws)
        self.space = space
        self.hints = hints or hints
        self.active = False

    def __str__(self):
        return str(self.id)

    def __repr__(self):
        return repr(self.id)


class Space(Idd):
    __by__ = 'tp'
    """ topology info for a space, combining units in one process """
    def __init__(self, tp, units=None, pars=None, **kws):
        super().__init__(**kws)
        self.tp = tp
        self.replicate = 0
        self.units = units or []
        self.pars = pars or set()
        self.bound = False

    def __str__(self):
        units = self.units
        if len(units) > 4:
            units = units[:2] + ['..'] + units[-1:]
        return '{{{}}}'.format(','.join(map(str, units)))

    def __repr__(self):
        return '{}@{!r}'.format(self, self.id)


class Port(Idd):
    """ topology info for a port """

    __by__ = 'unit', 'port'

    def __init__(self, unit, port, links=None, hints=None, **kws):
        super().__init__(**kws)
        self.unit = unit
        self.port = port
        self.links = links or []
        self.hints = hints or {}

    @property
    def pid(self):
        return self.id.idd

    def of(self, unit):
        assert unit.id == self.unit.id
        return getattr(unit, self.port)

    def __str__(self):
        return '{}.{}'.format(self.unit, self.port)

    def __repr__(self):
        return '{!r}.{}'.format(self.unit, self.port)


class Bundle:
    def __init__(self, items=[]):
        self.items = list(items)

    def units(self):
        for item in self.items:
            if isinstance(item, Unit):
                yield item
            else:
                yield from item.units


class Link:
    """ topology info for a link """
    def __init__(self, source, target, hints=None, **kws):
        super().__init__(**kws)
        self.source = source
        self.target = target
        self.hints = hints or {}

    @delayed
    def endpoint(self):
        return self.target.hints['sync'](self.source, self.target)

    @delayed
    def kind(self):
        if (self.target.unit.space == self.source.unit.space or
                not self.target.unit.space.bound and not self.source.unit.space.bound):
            return 'local'
        elif self.target.unit.space.replicate:
            return 'repl'
        else:
            return 'par'

    def __str__(self):
        return '{}>>{}'.format(self.source, self.target)

    def __repr__(self):
        return '{!r}>>{!r}'.format(self.source, self.target)

def ddict():
    return defaultdict(dict)

def dlist():
    return defaultdict(list)

class Topology(Idd):
    """ the flow topology object """
    def __init__(self, name='ctx', **kws):
        super().__init__(name=name, idd=os.getpid(), **kws)

        self.units = {}
        self.spaces = {}

        self.ports = defaultdict(ddict)
        self.links = []

        self.outlinks = defaultdict(dlist)
        self.inlinks = defaultdict(dlist)

    def register(self, unit, **hints):
        """ register unit inside topology """
        s = Space(self)
        u = Unit(space=s, name=unit.name, hints=hints)

        s.units.append(u)
        self.spaces[s.id.idd] = s

        self.units[u.id.idd] = u
        unit.id = u.id

        for port in unit.ports:
            self.get_port(port)
        return u

    def unregister(self, unit):
        u = self.lookup(unit)
        self.units.pop(u.id.idd)

    def join(self, s1, s2, bound=True):
        """ join to spaces together """
        if s2.id.idd in s1.pars:
            raise ValueError("can't join spaces becaue they are already parted")

        if s2.replicate:
            if s1.replicate and s2.replicate != s1.replicate:
                raise ValueError("different replicate values for spaces!")
            s1.replicate = s2.replicate

        for u in s2.units:
            u.space = s1
            s1.units.append(u)
        #s2.units.clear()
        self.spaces.pop(s2.id.idd)

        for si in s2.pars:
            self.spaces[si].pars.remove(s2.id.idd)
            self.spaces[si].pars.add(s1.id.idd)
        s1.pars.update(s2.pars)
        s2.pars.clear()

        s1.bound |= bound | s2.bound
        return s1

    def par(self, s1, s2, bound=True):
        """ set two spaces apart """
        if s1 == s2:
            raise ValueError("can't part spaces because they are already joined")
        s1.pars.add(s2.id.idd)
        s2.pars.add(s1.id.idd)

        s1.bound |= bound
        s2.bound |= bound
        return s2

    def add_link(self, source, target, **hints):
        """ adds links between source and target port """
        src = self.get_port(source)
        tgt = self.get_port(target)

        link = Link(src, tgt, hints)
        src.links.append(link)
        tgt.links.append(link)
        self.links.append(link)

        ep = link.endpoint

        self.outlinks[src.id.idd][tgt.id.idd].append(link)
        self.inlinks[tgt.id.idd][src.id.idd].append(link)
        return link

    def remove_links(self, source, target):
        """ removes links between source and target port """
        src = self.get_port(source)
        tgt = self.get_port(target)

        outs = self.outlinks[src.id.idd].pop(tgt.id.idd)
        ins = self.inlinks[tgt.id.idd].pop(src.id.idd)

        assert outs == ins

        for link in outs:
            src.links.remove(link)
            tgt.links.remove(link)
            self.links.remove(link)

        return outs

    def lookup(self, unit):
        """ lookup topology of a unit """
        if not isinstance(unit, Id):
            unit = unit.id
        return self.units[unit.idd]

    def get_port(self, port):
        """ get topology info of a port """
        uid = port.__self__.id.idd
        unit = self.units[uid]
        name = port.__name__
        kind = port.__kind__
        try:
            return self.ports[unit.id.idd][kind][name]
        except KeyError:
            port = self.ports[unit.id.idd][kind][name] = Port(unit, name, [], port.hints)
            return port

    def ports_of(self, unit, kind=None):
        ports = self.ports[self.lookup(unit).id.idd]
        if kind:
            return list(ports[kind].values())
        else:
            return [p for ps in ports.values() for p in ps.values()]

    def __getitem__(self, ref):
        try:
            return self.lookup(ref)
        except (KeyError, AttributeError):
            pass

        try:
            return self.get_port(ref)
        except (AttributeError, KeyError):
            pass

    def links_for(self, ref=None, kind=None):
        """
        Parameters
        ----------
        ref : <tp>, default None
            any topological reference object (space, unit, port)

        kind : {None, 'source', 'target'}, default None
            kink of the link or None
        """
        if ref is None:
            return set(self.links)
        ref = self[ref] or ref

        if kind == None:
            kinds = ['source', 'target']
        else:
            kinds = [kind]

        links=set()
        for l in self.links:
            for k in kinds:
                port = getattr(l, k)
                if (port == ref or
                    port.unit == ref or
                    port.unit.space == ref):
                    links.add(l)
        return links

    def endpoints(self, ref=None, kind=None):
        eps = defaultdict(list)
        for l in self.links_for(ref, kind=kind):
            eps[l.endpoint].append(l)
        return eps

    def autojoin_spaces(self, unit):
        if unit.space.bound:
            return

        bound = set()
        unbound = set()
        for l in self.links_to(unit):
            space = l.source.unit.space
            (bound if space.bound else unbound).add(space)

        if len(bound) == 1:
            master, = bound
        else:
            master = unit.space

        for space in unbound:
            if space not in master.pars:
                self.join(master, space, bound=False)

    def links_from(self, ref):
        return self.links_for(ref, kind='source')

    def links_to(self, ref):
        return self.links_for(ref, kind='target')

    def points_from(self, ref):
        return self.endpoints(ref, kind='source')

    def points_to(self, ref):
        return self.endpoints(ref, kind='target')

    def unit_dependecies(self, unit):
        unit = self[unit]

        # bfs for units sources
        units = []
        todos = {unit}
        done = set()
        while todos:
            done.update(todos)
            units.extend(todos)
            todos = {l.source.unit for td in todos for l in self.links_to(td)}
            todos -= done
        return units

    def dependencies(self, unit, kind=None):
        units = self.unit_dependecies(unit)
        if kind is None:
            return units
        else:
            return [p for u in units for p in self.ports_of(u, kind=kind)]

    def __repr__(self):
        items=[]
        for space in self.spaces.values():
            its = []
            its.append('{!r}:'.format(space))
            for unit in space.units:
                ins = self.links_to(unit)
                outs = self.links_from(unit)

                its.append('  {!r}:'.format(unit))
                its.extend(['    {} << {!r}'.format(p.target.port, p.source)
                            for p in ins])
                its.extend(['    {} >> {!r}'.format(p.source.port, p.target)
                            for p in outs])
            items.append(its)

        return '\n\n'.join('\n'.join(its) for its in items)
