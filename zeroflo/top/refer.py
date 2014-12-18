from collections import namedtuple

class Ref(namedtuple("RefBase", 'name, id')):
    def __str__(self):
        return '*{}'.format(self.name)

    def __repr__(self):
        return '*{}-{}'.format(self.name, self.id)


def nameof(obj):
    """
    :param obj:
    :return: name of the object, either as an attribute or based on the class
    """
    if hasattr(obj, 'name'):
        return obj.name
    cls = type(obj)
    return cls.__name__


class Container:
    def __init__(self):
        self.items = {}
        self.refs = {}

    def register(self, item):
        try:
            return self.items[item]
        except KeyError:
            self.items[item] = ref = Ref(nameof(item), id(item))
            self.refs[ref] = item
            return ref

    def lookup(self, item):
        return self.items[item]

    def __getitem__(self, ref):
        return self.refs[ref]

    def remove(self, item):
        ref = self.items.pop(item)
        self.refs.pop(ref)
        return ref

    def __delitem__(self, ref):
        item = self.refs.pop(ref)
        self.items.pop(item)
        return item

    def __len__(self):
        return len(self.refs)

    def __iter__(self):
        return iter(self.items)
