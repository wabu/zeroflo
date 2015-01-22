from pyadds.meta import ops

from zeroflo.model.base import HasPorts, Model


class Links(Model):
    def __init__(self):
        super().__init__()
        self._links = []

    def link(self, source, target, **opts):
        link = (source, target, opts)
        self._links.append(link)

    def links(self, source=None, target=None):
        links = []
        # TODO better filter using unit/port/sets theirof
        for link in self._links:
            src, tgt, _ = link
            if source is None or src in source:
                if target is None or tgt in target:
                    links.append(link)
        return links

    def unlink(self, source, target):
        ...


class MayLink(HasPorts):
    @ops.opsame
    def __rshift__(self, other: HasPorts):
        res = self.__series__(other)
        for src in self.source_ports:
            for tgt in other.target_ports:
                self.model.link(src, tgt)
        return res
