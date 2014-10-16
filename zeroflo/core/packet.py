from collections import namedtuple

class Packet(namedtuple('Packet', 'data tag')):
    """class for packets transfered inside flow"""
    def _data_len(self):
        try:
            return len(self.data)
        except:
            return None

    def _data_hash(self):
        try:
            return hex(hash(self.data))
        except:
            return None

    def _data_str(self):
        l = self._data_len()
        h = self._data_hash()
        if l is None and hash is None:
            return None
        else:
            def ns(a): return '' if a is None else a
            return '{}:{}:{}'.format(
                    type(self.data).__name__, ns(l), ns(h))

    def __str__(self):
        ds = self._data_str()
        if ds is None:
            ds = '1:...'
        return "({}; {})".format(ds, str(self.tag))

    def __repr__(self):
        ds = self._data_str()
        if ds is None:
            ds = str(self.data).split('\n')[0]
            if len(ds) > 24:
                ds = ds[:21]+'...'
        return "({}; {})".format(ds, repr(self.tag))


    def __rshift__(self, port):
        return (yield from port.handle(port, self))

    
class Tag(dict):
    """
    tag with metainfo about the packet
    >>> a = Tag(a=1, foo='baz')
    >>> b = a.add(b=2, foo='bar')
    >>> a.foo
        'baz'
    >>> a.b
    >>> b.foo
        'bar'
    >>> None >> Tag(b)
    """
    def add(self, **kws):
        new = Tag(self)
        new.update(kws)
        return new

    @classmethod
    def new(cls, **kws):
        return cls(kws)

    def __rrshift__(self, data):
        return Packet(data, self)

    def __getattr__(self, name):
        if name[0] == '_':
            raise AttributeError
        return self.get(name, None)

    def __getitem__(self, item):
        if isinstance(item, tuple):
            return Tag({i: self[i] for i in item})
        else:
            return getattr(self, item)

    def __repr__(self):
        return ', '.join('{}: {}'.format(k,v) for k,v in sorted(self.items()))

    def __str__(self):
        return ','.join(map(str, sorted(self.keys())))
