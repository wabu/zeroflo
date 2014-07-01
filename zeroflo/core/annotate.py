from functools import wraps
import weakref

from asyncio import coroutine, gather

class Named:
    def __init__(self, name, **kws):
        super().__init__(**kws)
        self.name = name

class Annotate(Named):
    """ annotation that is transfromed to a class """
    def __new__(cls, definition):
        return wraps(definition)(super().__new__(cls))

    def __init__(self, definition):
        super().__init__(name=definition.__name__)
        self.definition = definition

class Conotate(Annotate):
    """ annotation that is defined as a coroutine """
    def __init__(cls, definition, *args, **kws):
        definition = coroutine(definition)
        super().__init__(definition, *args, **kws)

class Descr(Named):
    """ base fore building descriptors """
    def lookup(self, obj):
        raise NotImplementedError

    def has_entry(self, obj):
        dct,key = self.lookup(obj)
        return key in dct

    @classmethod
    def iter(desc, obj):
        cls = type(obj)
        for name in dir(cls):
            attr = getattr(cls, name)
            if isinstance(attr, desc):
                yield attr


class ObjDescr(Descr):
    """ decriptor mixin to putting values in objects dict """
    def __init__(self, name):
        super().__init__(name=name)
        self.entry = '_'+name

    def lookup(self, obj):
        return obj.__dict__, self.entry

class RefDescr(Descr):
    """ descriptor mixin based on weak reference from objects """
    def __init__(self, name):
        super().__init__(name)
        self.refs = weakref.WeakKeyDictionary()

    def lookup(self, obj):
        return self.refs, obj

class Get(Descr):
    """ get descriptor """
    def __get__(self, obj, objtype=None):
        if obj:
            dct,key = self.lookup(obj)
            try:
                return dct[key]
            except KeyError:
                val = self.__default__(obj)
                dct[key] = val
                return val
        else:
            return self

    def __default__(self, obj):
        raise NameError("Descriptor %s of %s object has to be set first" % (self.name, type(obj).__name__))

class Set(Descr):
    """ set/delete descriptor """
    def __set__(self, obj, value):
        if obj:
            dct,key = self.lookup(obj)
            dct[key] = value
            return value
        else:
            return self

    def __delete__(self, obj):
        dct,key = self.lookup(obj)
        dct.pop(key, None)


class Cached(Descr):
    """ descriptor evaluationing definition once """
    def __default__(self, obj):
        return self.definition(obj)

class shared(Annotate, Cached, ObjDescr, Get):
    """ evaluate once and share in obj dict """
    pass

class local(Annotate, Cached, RefDescr, Set, Get):
    """ evaluate locally again """
    pass 

class delayed(Annotate, Cached, ObjDescr, Set, Get):
    """ evaluate as needed """
    pass

class initialized(Conotate, RefDescr, Set, Get):
    """ call coroutine once at with `initialize` with supplied kwargs to get value """
    pass

@coroutine
def initialize(obj, **opts):
    """ call all `@initialized` descriptors to initialize values """
    calls = []
    for desc in initialized.iter(obj):
        if desc.has_entry(obj):
            @coroutine
            def init():
                val = yield from desc.definition(obj, **opts)
                desc.__set__(obj, val)
                return attr.name, val

    return dict((yield from gather(*calls)))
