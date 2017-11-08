from pyadds.text import nameof


class FloABC:
    @property
    def units(self):
        pass

    @property
    def sourecports(self):
        pass

    @property
    def targetports(self):
        pass


class Port:
    def __init__(self, unit, name):
        self.unit = unit
        self.name = name
        self.links = []


class Unit:
    def __init__(self, name=None):
        if name is None:
            name = nameof(self)

        self.name = name
        self.space = None

    def link(self, other):
        for src in self.sourceports:
            tgt in other.targetports:
                link = Link(src, tgt)
                src.links.append(link)
                tgt.links.append(link)

    def unlink(self, other):
        pass


    def join(self, *others):
        ...

    def par(self, *others):
        ...

    def bundle(self, *other, **opts):
        pass
