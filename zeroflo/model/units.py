from zeroflo.model.base import ModelBase


class Units(ModelBase):
    def __init__(self):
        super().__init__()
        self._units = []

    def units(self):
        return self._units

    def unify(self, other):
        assert isinstance(other, Units)
        super().unify(other)
        self._units.extend(other._units)

    def register(self, *units):
        super().register(*units)
        for u in units:
            if u not in self:
                self._units.append(u)

    def unregister(self, unit):
        super().unregister(unit)
        self._units.remove(unit)

    def __str__(self):
        return '|{}|'.format(','.join(map(str, self._units)))

    def __repr__(self):
        return '|{:x}:{}|'.format(id(self),
                                  ','.join(map(str, self._units)))