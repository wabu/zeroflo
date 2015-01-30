from pyadds.meta import ops
from pyadds import Anything

from zeroflo.model.base import RequirePorts, ModelBase


class Link:
    """ single link between a source and an target port """
    def __init__(self, source, target, **opts):
        self.source = source
        self.target = target
        self.opts = opts

    def __iter__(self):
        return iter((self.source, self.target, self.opts))

    def __str__(self):
        return '{}>>{}'.format(self.source, self.target)

    def __repr__(self):
        return '{!r}>>{!r}//{}'.format(self.source, self.target, self.opts)


class Links(ModelBase):
    """
    models links between units
    """
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
        for link in self.links(sources={unit}) + self.links(targets={unit}):
            self.unlink(link.source, link.target)
        super().unregister(unit)

    def unify(self, other):
        assert isinstance(other, Links)
        super().unify(other)
        self._links += other._links


class BuildLinks(RequirePorts):
    """
    builds up links between ports using the `a >> b` syntax
    """
    @ops.opsame
    def __rshift__(self, other: RequirePorts):
        res = self.__series__(other)
        for src in self.source_ports:
            for tgt in other.target_ports:
                self.model.link(src, tgt)
        return res
