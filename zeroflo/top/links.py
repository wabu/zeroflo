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
        # {unit: (link)}
        self._source_refs = defaultdict(tuple)
        self._target_refs = defaultdict(tuple)

    def link(self, source, target):
        link = Link(source, target)

        self._source_refs[source.unit] += (link,)
        self._target_refs[target.unit] += (link,)
        return link

    @property
    def links(self):
        return sum(self._source_refs.values(), ())

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
        links = self._source_refs[unit] + self._target_refs[unit]
        for link in links:
            self.unlink(link)
        super().unregister(unit)

    def update(self, other):
        super().update(other)
        self._source_refs.update(other._source_refs)
        self._target_refs.update(other._target_refs)


@ops.defines
class Linking(UsePorts):
    def __init__(self, obj):
        super().__init__(obj)
        self.links = obj.links # top.Links()

    @ops.opsame
    def __rshift__(self, other):
        """ Links components. """
        for src in self.sources:
            for tgt in other.targets:
                self.links.link(src, tgt)
        return self.__serial__(other)


