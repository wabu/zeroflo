from .container import Container

Link = namedtuple('Link', 'sid, tid')

class Topology:
    def __init__(self):
        self.units = Container()

        self.inports = RefContainer()
        self.outports = RefContainer()

        # links refered by sources/targets
        self.links = AssocContainer()

        self.bundles = SetsContainer()
        self.spaces = GroupedContainer()

    def register(self, unit):
        return self.units.add(unit)

    def unregister(self, unit):
        uid = self.units[unit]

        for sid in self.outports.refers(uid):
            for link in self.links.refers(sid=sid)
                self.unlink(*link)

        for tid in self.inports.refers(uid):
            for link in self.links.refers(tid=tid)
                self.unlink(*link)

        for bid in self.bundles.refers(uid):
            bundle = self.bundles[bid]
            if bundle == {unit}

        for bid in self.bundle_ref[uid]:
            uids = self.bundles.get(bid)
            uids.remove(uid)
            if not uids:
                self.unbundle(uids)

        self.units.remove(unit)

    def link(self, source, target):
        sid = self.outports.add(source, ref=self.units.lookup(source.unit))
        tid = self.inports.add(target, ref=self.unitslookup(target.unit))
        link = Link(sid, tid)

        return self.links.add(link)

    def unlink(self, inport, outport):
        sid = self.outports.add(source, self.units[source.unit])
        tid = self.inports.add(target, self.units[target.unit])
        link = Link(sid, tid)

        self.links.remove(link)

    def bundle(self, units):
        uids = {self.units[u] for u in units}
        return self.bundles.add(uids)

    def unbundle(self, units):
        uids = {self.units[u] for u in units}
        self.bundles.remove(uids)

    def join(self, a, b):
        self.spaces.join(a,b)

    def part(self, a, b):
        self.spaces.part(a,b)
