from functools import wraps

def unit_ctx(f):
    @wraps(f)
    def insert_ctx(self, *args, **kws):
        return f(self, *args, ctx=self.__fl__.ctx, **kws)
    return insert_ctx

def flmethod(f):
    @wraps(f)
    def as_fl(self, *args, **kws):
        return f(self.__fl__, *args, **kws)
    return as_fl

def nids(fls):
    return {fl.nid for fl in fls}

class Named:
    __name__ = None
    def __init__(self, name=None, **kws):
        super().__init__(**kws)

        self.name = name or self.__name__ or type(self).__name__
        self.nid = '{}@{}'.format(self.name, ('%x' % id(self))[2:-2])

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.nid



