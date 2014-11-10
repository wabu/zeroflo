from .container import (Container,
                        RefContainer,
                        AssocContainer,
                        SetsContainer,
                        GroupedContainer)

from collections import namedtuple

Link = namedtuple('Link', 'sid, tid')


class Topology:
    def __init__(self):
        self.units = Container()

        self.tgtports = RefContainer()
        self.srcports = RefContainer()

        self.links = AssocContainer()

        self.bundles = SetsContainer()
        self.spaces = GroupedContainer()

    def register(self, unit):
        return self.units.add(unit)

    def unregister(self, unit):
        uid = self.units.lookup(unit)

        for sid in self.srcports.refers(uid):
            for link in self.links.refers(sid=sid):
                self.unlink(*link)
            self.srcports.remove(sid)

        for tid in self.tgtports.refers(uid):
            for link in self.links.refers(tid=tid):
                self.unlink(*link)
            self.tgtports.remove(tid)

        # TODO broadcast as we use container methods
        self.bundels.dismiss(uid)
        self.spaces.dismiss(uid)

        self.units.remove(unit)

    def link(self, source, target):
        sid = self.srcports.add(source, ref=self.units.lookup(source.unit))
        tid = self.tgtports.add(target, ref=self.units.lookup(target.unit))
        link = Link(sid, tid)

        return self.links.add(link)

    def unlink(self, source, target):
        sid = self.srcports.add(source, ref=self.units.lookup(source.unit))
        tid = self.tgtports.add(target, ref=self.units.lookup(target.unit))
        link = Link(sid, tid)

        self.links.remove(link)

    def bundle(self, units):
        uids = {self.units.lookup(u) for u in units}
        return self.bundles.add(uids)

    def unbundle(self, units):
        uids = {self.units.lookup(u) for u in units}
        self.bundles.remove(uids)

    def join(self, *units):
        uids = {self.units.lookup(u) for u in units}
        self.spaces.join(*uids)

    def part(self, *units):
        uids = {self.units.lookup(u) for u in units}
        self.spaces.part(*uids)
