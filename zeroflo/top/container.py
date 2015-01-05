from functools import reduce
from collections import defaultdict


class Ref(tuple):
    def __new__(cls, name, id):
        return super().__new__(cls, (name, id))

    @property
    def name(self):
        return self[0]

    @property
    def id(self):
        return self[1]

    def __str__(self):
        return '*{}'.format(self.name)

    def __repr__(self):
        return '*{}-{}'.format(self.name, self.id)


class Container:
    """
    containter class creating static element ids sparsly

    >>> cnt = Container(['a','b','c'])
    >>> cnt
        [0: 'a',
         1: 'b',
         2: 'c']
    >>> cnt[2]
        'c'
    >>> cnt.lookup('a')
        0
    >>> cnt.remove('b')
        1
    >>> cnt
        [0: 'a',
         2: 'c']
    >>> cnt.add('d')
        1
    >>> cnt
        [0: 'a',
         1: 'd',
         2: 'c']
    >>> cnt.add('d')
        1
    """
    def __init__(self, items=[]):
        self._free = []
        self._items = {}
        self._nums = {}

        self.extend(items)

    def add(self, item):
        try:
            return self._nums[item]
        except KeyError:
            if self._free:
                i = self._free.pop()
            else:
                i = len(self._nums)
            self._nums[item] = i
            self._items[i] = item
            return i

    def extend(self, items):
        for i in items:
            self.add(i)

    def lookup(self, item):
        return self._nums[item]

    def remove(self, item):
        i = self._nums.pop(item)
        self._items.pop(i)
        self._free.append(i)
        return i

    def __getitem__(self, i):
        return self._items[i]

    def __delitem__(self, i):
        item = self[i]
        self.remove(item)

    def __iter__(self):
        return map(lambda x: x[1], self.items())

    def items(self):
        return sorted(self._items.items())

    def __str__(self):
        return '[{}]'.format(', '.join(map(str, self)))

    def __repr__(self):
        its = ('{}: {}'.format(*it) for it in self.items())
        return '[{}]'.format('\n '.join(its))


class RefersMixin:
    def _remove_ref(self, ref, i, refdct=None):
        if refdct is None:
            refdct = self._refers

        refdct[ref].remove(i)
        if not refdct[ref]:
            refdct.pop(ref)
        return i

    def _reduce_refs(self, refsets):
        return reduce(set.intersection, refsets)


class RefContainer(RefersMixin, Container):
    """
    containter with elements refered by an outside reference
    """
    def __init__(self, items={}):
        self._refers = defaultdict(set)
        self._refered = defaultdict(set)
        super().__init__(items=items)

    def add(self, item, ref):
        i = super().add(item)
        self._refers[ref].register(i)
        self._refered[i].register(ref)
        return i

    def extend(self, items):
        for item, ref in items.items():
            self.add(item, ref)

    def remove(self, item):
        i = super().remove(item)
        for ref in self._refered[i]:
            self._remove_ref(ref, i)
        return i

    def refers(self, ref):
        return self._reduce_refs([self._refers[ref]])


class AssocContainer(RefersMixin, Container):
    """
    containter where elements are namedtuples with components refering to tuple
    """
    def __init__(self, items=[]):
        self._assocs = defaultdict(lambda: defaultdict(set))
        super().__init__(items=items)

    def add(self, item):
        i = super().add(item)
        for name, ref in item._asdict().items():
            self._assocs[name][ref].register(i)
        return i

    def remove(self, item):
        i = super().remove(item)
        for name, ref in item._asdict().items():
            self._remove_ref(ref, i, refdct=self._assocs[name])
        return i

    def refers(self, **refs):
        return self._reduce_refs(self._assocs[name].get(val, set())
                                 for name, val in refs.items())


class SetsContainer(RefersMixin, Container):
    """
    containter where items of element sets refer to there containing sets
    """
    def __init__(self, items=[]):
        self._refers = defaultdict(set)
        super().__init__(items=items)

    def add(self, items):
        items = tuple(set(items))
        i = super().add(items)
        for ref in items:
            self._refers[ref].register(i)
        return i

    def lookup(self, items):
        items = tuple(set(items))
        return super().lookup(items)

    def __getitem__(self, i):
        return set(super().__getitem__(i))

    def remove(self, items):
        items = tuple(set(items))
        i = super().remove(items)
        for ref in items:
            self._remove_ref(ref, i)
        return i

    def refers(self, *items):
        return self._reduce_refs(self._refers[ref] for ref in items)

    def dismiss(self, item):
        for i in self.refers(item):
            items = self[i]
            self.remove(items)
            items.remove(item)
            if items:
                j = self.add(items)
                assert i == j
            yield i, items


class GroupedContainer(SetsContainer):
    def join(self, *items):
        new = set(items)
        grps = [grp for grp in self
                if new.intersection(grp)]

        if len(grps) > 1:
            raise IndexError('items of different groups cannot be joined')
        elif grps:
            grp, = grps
            i = self.remove(grp)
            j = self.add(new.union(grp))
            assert i == j
        else:
            self.add(items)

    def part(self, *items):
        new = set(items)
        for grp in self:
            if len(new.intersection(grp)) > 1:
                raise IndexError('cannot part items of the same group')
            new -= set(grp)

        for item in new:
            self.add({item})
