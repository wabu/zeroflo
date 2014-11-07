from functools import reduce
from collections import namedtuple, defaultdict

class Container:
    """
    containter class creating element ids sparsly

    >>> c = Container()
    >>> c.add('a')
        0
    >>> c.add('b')
        1
    >>> c.add('c')
        2
    >>> c[2]
        'c'
    >>> c.lookup('a')
        0
    >>> c
        [0: 'a', 
         1: 'b', 
         2: 'c']
    >>> c.remove('b')
        1
    >>> c
        [0: 'a', 
         2: 'c']
    >>> c.add('d')
        1
    >>> c
        [0: 'a',
         1: 'd',
         2: 'c']
    >>> c.add('d')
        1
    """
    def __init__(self):
        self._free = []
        self._nums = {}
        self._items = {}

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

    def lookup(self, item):
        return self._nums[item]

    def remove(self, item):
        i = self._nums.pop(item)
        self._items.pop(i)
        self._free.append(i)
        return i

    def __getitem__(self, i):
        return self._items[i]

    def __iter__(self):
        return iter(self._nums.keys())

    def items(self):
        for item, i in self._nums.items():
            yield i,item


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
    def __init__(self):
        super().__init__()
        self._refers = defaultdict(set)
        self._refered = defaultdict(set)

    def add(self, item, ref):
        i = super().add(item)
        self._refers[ref].add(i)
        self._refered[i].add(ref)
        return i

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
    def __init__(self):
        super().__init__()
        self._assocs = defaultdict(lambda: defaultdict(set))


    def add(self, item):
        i = super().add(item)
        for name,ref in item._asdict().items():
            self._assocs[name][ref].add(i)

    def remove(self, item):
        i = super().remove(item)
        for name,ref in item._asdict().items():
            self._remove_ref(ref, i, refdct=self._assocs[name])
        return i

    def refers(self, **refs):
        return self._reduce_refs(self._assocs[name] for name,_ in refs.items())


class SetsContainer(RefersMixin, Container):
    """
    containter where items of element sets refer to there containing sets
    """
    def __init__(self):
        super().__init__()
        self._refers = defaultdict(set)

    def add(self, items):
        items = tuple(set(items))
        i = super().add(items)
        for ref in items:
            self._refers[ref].add(i)

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


class GroupedContainer(SetsContainer):
    def join(self, *items):
        grps = [grp for grp in self
                if grp.intersection(items)]

        if len(grps) > 1:
            raise IndexError('items of different groups cannot be joined')
        elif grps:
            grp, = grps
            i = self.remove(grp)
            j = self.add(grp.union(items))
            assert i==j
        else:
            self.add(items)
        
    def part(self, *items):
        new = set(items)
        for grp in self:
            if len(grp.intersection(items)) > 1:
                raise IndexError('cannot part items of the same group')
            new -= grp

        for item in new:
            self.add({item})
            
