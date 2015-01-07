from .container import (Container,
                        ReferedContainer,
                        AssocContainer,
                        SetsContainer,
                        GroupedContainer)

from collections import namedtuple

Link = namedtuple('Link', 'sid, tid')


class Topology:
    def __init__(self):
        self.units = Container()

        self.tgtports = ReferedContainer()
        self.srcports = ReferedContainer()

        self.links = AssocContainer()

        self.bundles = SetsContainer()
        self.spaces = GroupedContainer()

    def register(self, unit):
        return self.units.add(unit)

    def unregister(self, unit):
        uid = self.units.index(unit)

        for sid in self.srcports.refered_keys(uid):
            for link in self.links.refered_values(sid=sid):
                self.unlink(*link)
            self.srcports.remove(sid)

        for tid in self.tgtports.refered_keys(uid):
            for link in self.links.refered_values(tid=tid):
                self.unlink(*link)
            self.tgtports.remove(tid)

        # TODO broadcast as we use container methods
        self.bundles.dismiss(uid)
        self.spaces.dismiss(uid)

        self.units.remove(unit)

    def link(self, source, target):
        sid = self.srcports.add(source, ref=self.units.index(source.unit))
        tid = self.tgtports.add(target, ref=self.units.index(target.unit))
        link = Link(sid, tid)
        self.links.add(link)
        return link

    def unlink(self, source, target):
        sid = self.srcports.add(source, ref=self.units.index(source.unit))
        tid = self.tgtports.add(target, ref=self.units.index(target.unit))
        link = Link(sid, tid)
        self.links.remove(link)

    def bundle(self, units):
        uids = {self.units.index(u) for u in units}
        return self.bundles.add(uids)

    def unbundle(self, units):
        uids = {self.units.index(u) for u in units}
        self.bundles.remove(uids)

    def join(self, *units):
        uids = {self.units.index(u) for u in units}
        self.spaces.join(*uids)

    def part(self, *units):
        uids = {self.units.index(u) for u in units}
        self.spaces.part(*uids)
