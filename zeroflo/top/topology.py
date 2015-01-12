from .container import (Container,
                        ReferedContainer,
                        AssocContainer,
                        SetsContainer,
                        GroupedContainer)

from collections import namedtuple

Link = namedtuple('Link', 'sid, tid')


class Topology:
    def __init__(self):
        self.units = Container('unit')

        self.tgtports = ReferedContainer('in')
        self.srcports = ReferedContainer('out')

        self.links = AssocContainer('link')

        self.bundles = SetsContainer('bundle')
        self.bundle_opts = {}
        self.spaces = GroupedContainer('space')

    def register(self, unit):
        return self.units.add(unit)

    def unregister(self, unit):
        uid = self.units.index(unit)

        for sid in self.srcports.refered_keys(uid):
            for link in self.links.refered_values(sid=sid):
                self.unlink(self.srcports[sid], self.tgtports[link.tid])
            del self.srcports[sid]

        for tid in self.tgtports.refered_keys(uid):
            for link in self.links.refered_values(tid=tid):
                self.unlink(self.srcports[link.sid], self.tgtports[tid])
            del self.tgtports[tid]

        for bid in list(self.bundles.refered_ids(uid)):
            bdl = self.bundles.pop(bid)
            opts = self.bundle_opts.pop(bid)
            bdl.remove(uid)
            if bdl:
                self.bundle(bdl, **opts)

        # TODO broadcast as we use container methods
        self.bundles.dismiss(uid)
        self.spaces.dismiss(uid)

        self.units.remove(unit)

    def link(self, source, target):
        sid = self.srcports.add(source, ref=self.units.add(source.unit))
        tid = self.tgtports.add(target, ref=self.units.add(target.unit))
        link = Link(sid, tid)
        self.links.add(link)
        return link

    def unlink(self, source, target):
        sid = self.srcports.add(source, ref=self.units.index(source.unit))
        tid = self.tgtports.add(target, ref=self.units.index(target.unit))
        link = Link(sid, tid)
        self.links.remove(link)

    def bundle(self, units, **opts):
        uids = {self.units.add(u) for u in units}
        ref = self.bundles.add(uids)

        bdl_opts = self.bundle_opts.setdefault(ref.id, {})
        self._bdl_update(units, bdl_opts, opts)
        return ref

    def _bdl_update(self, units, bdl, opts):
        conflicts = [key
                     for key, val in opts.items()
                     if key in bdl and bdl[key] != val]
        if conflicts:
            raise IndexError('conflicting values for {} of bundle {}'.format(
                ', '.join(conflicts), set(units)))
        bdl.update(opts)

    def unbundle(self, units):
        uids = {self.units.index(u) for u in units}
        ref = self.bundles.remove(uids)
        return self.bundle_opts.pop(ref.id)

    def join(self, *units):
        uids = {self.units.add(u) for u in units}
        self.spaces.join(*uids)

    def part(self, *units):
        uids = {self.units.add(u) for u in units}
        self.spaces.part(*uids)

    def __repr__(self):
        its = []
        for sid, uids in self.spaces.items():
            its.append('{!r}:'.format(sid))
            for uid in uids:
                unit = self.units[uid]
                its.append('  {!r} ({!r}):'.format(unit, uid))

                for tid, target in self.tgtports.refered_items(uid):
                    for sid, _ in self.links.refered_values(tid=tid):
                        source = self.srcports[sid]
                        its.append('    {} >> {}'.format(source, target))

                for sid, source in self.srcports.refered_items(uid):
                    for _, tid in self.links.refered_values(sid=sid):
                        target = self.tgtports[tid]
                        its.append('    {} >> {}'.format(source, target))

        its.append('')
        for bid, bundle in self.bundles.items():
            opts = self.bundle_opts[bid.id]
            its.append('{}:\n    {}'.format(bundle, opts))

        return '\n'.join(its)
