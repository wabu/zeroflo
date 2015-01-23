from pyadds.meta import ops
from pyadds import Anything

from zeroflo.model.base import RequirePorts, ModelBase

class Link:
    def __init__(self, source, target, **opts):
        self.source = source
        self.target = target
        self.opts = opts

    def __iter__(self):
        return iter((self.source, self.target, self.opts))


class Links(ModelBase):
    def __init__(self):
        super().__init__()
        self._links = []

    def link(self, source, target, **opts):
        link = Link(source, target, **opts)
        self._links.append(link)

    def links(self, sources=Anything, targets=Anything):
        links = []
        for link in self._links:
            src, tgt, _ = link
            if any(s in sources for s in (src, src.unit)):
                if any(t in targets for t in (tgt, tgt.unit)):
                    links.append(link)
        return links

    def unlink(self, source, target):
        for link in self.links(sources={source}, targets={target}):
            self._links.remove(link)

    def unregister(self, unit):
        for link in self.links:
            source, target, _ = link
            if unit in {source.unit, target.unit}:
                self.unlink(link)
        super().unregister(unit)

    def update(self, other):
        super().update(other)
        self._links += other._links


class BuildLinks(RequirePorts):
    @ops.opsame
    def __rshift__(self, other: RequirePorts):
        res = self.__series__(other)
        for src in self.source_ports:
            for tgt in other.target_ports:
                self.model.link(src, tgt)
        return res
