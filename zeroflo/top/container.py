from collections import defaultdict
from functools import reduce


class IdGen:
    def __init__(self):
        self.next_id = 0

    def add(self, id):
        if id < self.next_id:
            raise ValueError('conflicting id')
        self.next_id = id + 1

    def next(self):
        id = self.next_id
        self.next_id += 1
        return id

    def free(self, id):
        pass


class ReusingGen(IdGen):
    def __init__(self):
        super().__init__()
        self.free_ids = []

    def add(self, id):
        if id in self.free_ids:
            self.free_ids.remove(id)
        else:
            super().add(id)

    def next(self):
        if self.free_ids:
            return self.free_ids.pop(0)
        else:
            return super().next()

    def free(self, id):
        self.free_ids.append(id)


class RefId(int):
    @property
    def id(self):
        return int(self)

    def __bool__(self):
        return True

    def __eq__(self, other):
        if not self.cid == other.cid:
            return False
        return super().__eq__(other)

    def __ne__(self, other):
        if not self.cid != other.cid:
            return True
        return super().__eq__(other)

    def __hash__(self):
        return self ^ hash(self.cid << 16)

    def __str__(self):
        return '*{}-{}'.format(self.name, super().__str__())

    __repr__ = __str__


class Container:
    def __init__(self, name=None):
        self.RefId = type('RefId{:X}'.format(id(self)), (RefId,),
                          {'name': name or 'cnt', 'cid': id(self)})
        self._by_id = {}
        self._by_item = {}
        self._ids = ReusingGen()

    def _key(self, item):
        return item

    def _insert(self, id, key, item, *args, **kws):
        """insert id, key item into container with additional args"""
        self._by_id[id] = item
        self._by_item[key] = id

    def _remove(self, id=None, item=None):
        """remove an item by id or item form the container"""
        assert (item or id) is not None, 'either item or id has to be given'
        if not id:
            key = self._key(item)
            id = self._by_item[key]
        if not item:
            id = int(id)
            item = self._by_id[id]
            key = self._key(item)

        self._ids.free(id)
        del self._by_item[key]
        del self._by_id[id]
        return id, item

    def add(self, item, *args, **kws):
        """
        adds in item to the container and returns a ref-id
        :param item: item to add
        :return: int that is unique to the container for the item
        """
        key = self._key(item)
        try:
            id = self._by_item[key]
        except KeyError:
            id = self._ids.next()
            self._insert(id, key, item, *args, **kws)
        return self.RefId(id)

    def __setitem__(self, id, item):
        """
        try to add an item with a given id
        :param id: id to use
        :param item: item to add
        :raises IndexError: id conflicts with ids in container
        """
        id = int(id)
        args = []
        if isinstance(item, slice):
            item = slice.start
            args.append(item.stop)
        try:
            if item != self._by_id[id]:
                raise IndexError('conflicting ids')
        except KeyError:
            key = self._key(item)
            self._ids.add(id)
            self._insert(id, key, item, *args)

    def remove(self, item):
        """
        removes an item from the container
        :param item: item to be removed
        :return: id of the item that is removed
        :raises KeyError: item not inside the container
        """
        id, _ = self._remove(item=item)
        return self.RefId(id)

    def index(self, item):
        """
        :param item: item in the container
        :return: ref-id of the item
        :raises KeyError: item not inside the container
        """
        return self.RefId(self._by_item[item])

    def __getitem__(self, id):
        """
        get item with a given id
        :param id: int or ref-id for an item
        :return: item for id
        :raises KeyError: id is not known
        """
        return self._by_id[int(id)]

    def __delitem__(self, id):
        """
        remove an item with a given id
        :param id: int or ref-id for an item
        :return: item for id
        :raises KeyError: id is not known
        """
        _, item = self._remove(id=id)
        return item

    def __iter__(self):
        """
        Iterator for values in the container
        """
        return iter(self.values())

    def __contains__(self, item):
        """
        Test if an item is in the container
        :param item: item to look for
        :return: bool
        """
        return self._key(item) in self._by_item

    def __len__(self):
        """
        Number of elements in the set
        """
        return len(self._by_id)

    def keys(self):
        """
        Get ref-ids of the items in the container
        :return: ref-ids
        """
        return map(self.RefId, self._by_id.keys())

    def values(self):
        """
        Get values of the items in the container
        :return: items
        """
        return self._by_id.values()


class RefersMixin:
    def _remove_ref(self, ref, id, refdct=None):
        """remove an id for an ref entry"""
        if refdct is None:
            refdct = self._refers

        refdct[ref].remove(id)
        if not refdct[ref]:
            refdct.pop(ref)

    def refered_ids(self, *args, **kws):
        raise NotImplementedError

    def refered_keys(self, *args, **kws):
        """
        get ref-ids for items as refered by arguments
        :return: list of ref-ids
        """
        return [self.RefId(id) for id in self.refered_ids(*args, **kws)]

    def refered_values(self, *args, **kws):
        """
        get values for items as refered by arguments
        :return: list of items
        """
        return [self[id] for id in self.refered_ids(*args, **kws)]

    def dismiss(self, *args, **kws):
        """
        remove all items as refered by arguments
        """
        for id in list(self.refered_ids(*args, **kws)):
            del self[self.RefId(id)]


class ReferedContainer(RefersMixin, Container):
    def __init__(self, name=None):
        self._refers = defaultdict(set)
        self._referer = dict()
        super().__init__(name=name or 'refed')

    def _insert(self, id, key, item, ref=None):
        super()._insert(id, key, item)
        self._refers[ref].add(id)
        self._referer[id] = ref

    def _remove(self, id=None, item=None):
        id, item = super()._remove(id, item)
        self._remove_ref(self._referer[id], id)
        return id, item

    def refered_ids(self, ref):
        return self._refers[ref]


class AssocContainer(RefersMixin, Container):
    def __init__(self, name=None):
        self._assocs = defaultdict(lambda: defaultdict(set))
        super().__init__(name=name or 'assoc')

    def _insert(self, id, key, item):
        super()._insert(id, key, item)
        for name, ref in item._asdict().items():
            self._assocs[name][ref].add(id)

    def _remove(self, id=None, item=None):
        id, item = super()._remove(id, item)
        for name, ref in item._asdict().items():
            self._remove_ref(ref, id, refdct=self._assocs[name])
        return id, item

    def refered_ids(self, **refs):
        rs = (self._assocs[name].get(val, set()) for name, val in refs.items())
        return reduce(set.intersection, rs)


class SetsContainer(RefersMixin, Container):
    def __init__(self, name=None):
        self._refers = defaultdict(set)
        super().__init__(name=name or 'sets')

    def _key(self, item):
        return tuple(sorted(set(item)))

    def _insert(self, id, key, item):
        super()._insert(id, key, item)
        for elem in item:
            self._refers[elem].add(id)

    def _remove(self, id=None, item=None):
        if item is not None:
            item = tuple(set(item))
        id, item = super()._remove(id, item)
        for elem in item:
            self._remove_ref(elem, id)
        return id, item

    def refered_ids(self, *items):
        rs = (self._refers[elem] for elem in items)
        return reduce(set.intersection, rs)


class GroupedContainer(SetsContainer):
    def join(self, *items):
        """
        join items into a single group of this container
        :param items: items to insert as group
        :raises IndexError: if some items are already in different groups
        """
        new = set(items)
        grps = [grp for grp in self if new.intersection(grp)]

        if len(grps) > 1:
            raise IndexError('items of diefferent groups cannot be joined')
        elif grps:
            grp, = grps
            i = self.remove(grp)
            self[i] = new.union(grp)
        else:
            self.add(new)

    def part(self, *items):
        """
        puts items into different groups
        :param items: items to insert into different groups
        :raises IndexError: if some items are already in same group
        """
        new = set(items)
        for grp in self:
            if len(new.intersection(grp)) > 1:
                raise IndexError('cannot part items of the same group')
            new -= set(grp)

        for item in new:
            self.add({item})
