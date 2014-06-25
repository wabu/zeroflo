from functools import wraps
import weakref

from asyncio import coroutine, gather

class Named:
    def __init__(self, name):
        self.name = name

class Annotate(Named):
    def __new__(cls, definition):
        return wraps(definition)(super().__new__(cls))

    def __init__(self, definition):
        super().__init__(name=definition.__name__)
        self.definition = definition

class Conotate(Annotate):
    def __init__(cls, definition, *args, **kws):
        definition = coroutine(definition)
        super().__init__(definition, *args, **kws)

class Descr(Named):
    def lookup(self, obj):
        raise NotImplementedError

    def has_entry(self, obj):
        dct,key = self.lookup(obj)
        return key in dct

class ObjDescr(Descr):
    def __init__(self, name):
        super().__init__(name=name)
        self.entry = '_'+name

    def lookup(self, obj):
        return obj.__dict__, self.entry

class RefDescr(Descr):
    def __init__(self, name):
        super().__init__(name)
        self.refs = weakref.WeakKeyDictionary()

    def lookup(self, obj):
        return self.refs, obj

class Get(Descr):
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
    def __default__(self, obj):
        return self.definition(obj)

class shared(Annotate, Cached, ObjDescr, Get):
    pass

class local(Annotate, Cached, RefDescr, Set, Get):
    pass 

class delayed(Annotate, Cached, ObjDescr, Set, Get):
    pass

class initialized(Conotate, RefDescr, Set, Get):
    pass

@coroutine
def initialize(obj, **opts):
    cls = type(obj)
    calls = []
    for attr in dir(cls):
        desc = getattr(cls, attr)
        if isinstance(desc, initialized):
            if not desc.has_entry(obj):
                @coroutine
                def init():
                    val = yield from desc.definition(obj, **opts)
                    desc.__set__(obj, val)
                    return val
                calls.append(init())

    yield from gather(*calls)
