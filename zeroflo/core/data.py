from queue import Empty, Full
import pickle

class LowData(Empty):
    pass

class MoreData(Full):
    pass

# XXX howto handle tags ...

class Datas:
    def __init__(self, tag=set()):
        self.tag = tag

    def empty(self):
        return []

    def append(self, ds, d):
        ds.append(d)
        return ds

    def length(self, ds):
        return len(ds)

    def pull(self, ds, low=None, high=None, flush=False):
        l = self.length(ds)
        if not l:
            raise LowData
        if flush and l>1:
            raise MoreData
        return ds[0], ds[1:]

    def pack(self, d):
        return pickle.dumps(d)

    def unpack(self, b):
        return pickle.loads(b)

    def map(self, d, by, num):
        if by in self.tag:
            yield hash(tag[by]) % num, ds


class Bytes(Datas):
    def __init__(self, sep=b'', tag=[]):
        super().__init__(tag)
        self.sep = sep

    def empty(self) :
        return bytearray()

    def append(self, ds, d : bytes):
        obj.append(self.sep)
        obj.extend(new)
        return obj

    def pull(self, ds, low=0, high=None, flush=False):
        if flush:
            return ds, self.empty()
        if low >= len(ds):
            raise LowData

        sep = self.sep
        if not sep:
            return ds[:high], ds[high:]

        sel = ds
        pre = suf = None
        if low:
            pre = ds[:low]
            sel = ds[low:]
        if high:
            suf = ds[high:]
            sel = sel[:high-low]

        data,*rest = sel.rsplit(sep, 1)
        if not rest and high:
            suf = None
            data,*rest = ds[low:].rsplit(sep, 1)

        if rest:
            rest = rest[0]
        else:
            raise LowData

        if pre:
            pre.append(data)
            data = pre
        if suf:
            rest.append(suf)
        return data,rest

    def pack(self, d):
        return d

    def unpack(self, b):
        return bytearray(b)


class Frames(Datas):
    def __init__(self, columns=[], **opts):
        super().__init__(**opts)
        self.columns = columns

    def empty(self):
        return pd.DataFrame(columns=[])

    def append(self, ds, d):
        return ds.append(d)

    def pull(self, ds, flush=None, high=None, low=None):
        if flush:
            return ds, self.empty()
        if len(ds) < low:
            raise LowData
        return ds.iloc[:high], ds.iloc[high:]

    def map(self, ds, by, num):
        if by in self.columns:
            grp = ds[by].apply(hash) % num
            yield from ds.grouby(by)
        else:
            yield from super().map(ds, by, num)


class Foo(Unit):
    @inport(Bytes)
    def process(self, data, tag) -> Bytes(sep=b'\n'):
        yield from ... >> tag.register(f00=1337) >> self.out

    @outport(Frames)
    def out(self, data, tag) -> Frames(columns=['a','b','c'], tag=process.tag+{'f00'}):
        pass


class Bar(Unit):
    @inport
    def process(self, data, tag) -> Frames(columns=['a', ...]):
        yield from ... >> tag >> self.out

    @outport
    def out(self, data, tag) -> Frames(columns=process.columns-['a'], tag=process.tag):
        pass

