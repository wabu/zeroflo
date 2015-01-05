from collections import namedtuple
from pyadds.str import name_of


class Ref(namedtuple("RefBase", 'name, id, base')):
    def __new__(cls, name, iid):
        return super().__new__(cls, name, iid, id(cls))

    def __str__(self):
        return '*{}'.format(self.name)

    def __repr__(self):
        return '*{}-{}'.format(self.name, self.id)


class Container:
    def __init__(self):
        self._free_ids = []
        self._next_id = 0

        self._items = {}
        self._refs = {}

        self.Ref = type('Ref', (Ref,), {'__del__': self._del_ref})

    def _mk_ref(self, name):
        if self._free_ids:
            iid = self._free_ids.pop(0)
        else:
            iid = self._next_id
            self._next_id += 1
        return self.Ref(name, iid)

    @property
    def _del_ref(self):
        def _del(ref):
            self._free_ids.append(ref.id)
        return _del

    def register(self, item):
        try:
            return self._items[item]
        except KeyError:
            self._items[item] = ref = self._mk_ref(name_of(item))
            self._refs[ref.id] = item
            return ref

    def lookup(self, item):
        return self._items[item]

    def __getitem__(self, ref):
        return self._refs[ref.id]

    def remove(self, item):
        ref = self._items.pop(item)
        self._refs.pop(ref.id)
        return ref

    def refs(self):
        return self._items.values()

    def values(self):
        return self._refs.values()

    def __delitem__(self, ref):
        item = self._refs.pop(ref.id)
        self._items.pop(item)
        return item

    def __len__(self):
        return len(self._refs)

    def __iter__(self):
        return iter(self._items)
