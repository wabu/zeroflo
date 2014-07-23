from ..core.annotate import local
from ..core import asyncio, coroutine, Unit, inport, outport, delayed
from ..ext.params import Paramed, param

import pandas as pd
import asyncio
import aiohttp
import zlib

from collections import namedtuple
from functools import wraps

size_suffixes = ['k', 'm', 'g', 't', 'p']
def strtosize(s):
    try:
        if s[-1].isalpha():
            num = size_suffixes.index(s[-1].lower())+1
            s = s[:-1]
        else:
            num = 0

        s = float(s)
        s = int(s * 1024**num)
        return s
    except ValueError as e:
        print(e)
        return -1

class Stats(namedtuple('Stats', 'name, dir, modified, size')):
    """ simple ressource stats """
    def __str__(self):
        dct = {f: getattr(self, f) for f in self._fields}
        return '{name:<12} {typ} {modified} {size:12d}'.format(typ='d' if self.dir else 'f', **dct)

    __repr__ = __str__


class Ressource:
    """ ressource access """
    def __init__(self, path):
        self.path = path

    @property
    @coroutine
    def exists(self):
        """ check if ressource exists """
        return bool((yield from self.stat))

    @property
    @coroutine
    def stat(self):
        """ stats (name, dir, modified, size) of this ressource of None if not existent"""
        raise NotImplementedError

    @property
    @coroutine
    def size(self):
        """ ressource size """
        return (yield from self.stats).size

    @property
    @coroutine
    def modified(self):
        """ modification time """
        return (yield from self.stats).modified

    @coroutine
    def text(self, encoding=None):
        """ textual content as str """
        args = {}
        if encoding:
            args['encoding'] = encoding
        return (yield from self.bytes()).decode(**args)

    @coroutine
    def bytes(self):
        """ ressource content as bytes """
        reader = yield from self.reader()
        chunks = []
        while True:
            chunk = yield from reader.read()
            if not chunk:
                break
            chunks.append(chunk)
        return b''.join(chunks)

    @coroutine
    def reader(self):
        """ reader for this ressource """
        raise NotImplementedError


class Directory(Ressource):
    """ directory access """
    @coroutine
    def listen(self):
        """ list of all ressource in this directory """
        return [s.name for s in (yield from self.stats())]

    @coroutine
    def stats(self):
        """ stats (name, dir, modified, size) for ressource in directory """
        raise NotImplementedError

    @coroutine
    def await(self, name: str, poll=1.0):
        """ wait until the give ressource is created in this directory """
        ressource = self.open(name)
        while not (yield from ressource.exists):
            yield from asyncio.sleep(poll)
        return ressource

    def open(self, name: str):
        """ return Ressource object for ressource in this directory """
        raise NotImplementedError

    def dir(self, name: str):
        """ get a subdirectory """
        raise NotImplementedError


Location = namedtuple('Location', 'path, begin, end, available')

class LocateByTime():
    def __init__(self, format, *, width, delay='0s', **kws):
        self.width = width = pd.datetools.to_offset(width)
        self.delay = pd.datetools.to_offset(delay)
        self.format = format
        kws['width'] = width
        self.kws = kws

    @local
    def convert(self):
        kws = self.kws
        format = self.format

        def conversion(val):
            if isinstance(val, str):
                return lambda t: t.strftime(val)
            if isinstance(val, pd.offsets.Tick):
                nanos = val.nanos
                return lambda t: t.value // nanos
            raise ValueError("No conversion possible with %s" % type(val))
        args = {name: conversion(val) for name,val in kws.items()}
        def convert(time):
            return format.format(time=time, **{k: v(time) for k,v in args.items()})
        return convert

    def location(self, time='now'):
        time = pd.Timestamp(time)
        start = pd.Timestamp(time.value // self.width.nanos * self.width.nanos)
        return Location(self.convert(start), start, start+self.width, start+self.width+self.delay)


class Watch(Unit, Paramed):
    """ watch a ressource directory for located ressources becomming available """
    def __init__(self, root, locate, **kws):
        super().__init__(**kws)
        self.root = root
        self.locate = locate

    @param
    def stable(self, value='30s'):
        return pd.datetools.to_offset(value)

    @outport
    def out(): pass

    @inport
    def ins(self, start, tag):
        start = pd.Timestamp(start or tag.start or pd.Timestamp('now')-pd.datetools.Minute(30))
        end = pd.Timestamp(tag.end)

        time = pd.Timestamp(start)
        locate = self.locate
        root = self.root

        while not time > end:
            loc = locate.location(time)
            while True:
                wait = (loc.available - pd.Timestamp('now')).total_seconds()
                if wait <= 0:
                    break
                print('waiting %ds for %s' % (wait, loc.path), end='\033[J\r')
                yield from asyncio.sleep(1)

            last = 0
            while True:
                ressource = yield from root.await(loc.path)
                stat = yield from ressource.stat
                if stat: 
                    if stat.size == last or pd.Timestamp('now', tz='utc') > stat.modified + self.stable:
                        break
                    if last != 0:
                        print('%s not stable yet: %s -> %s' % (loc.path, last, stat.size), end='\033[J\r')
                    last = stat.size
                else:
                    diff = (pd.Timestamp('now') - loc.end).total_seconds()
                    print('%s still not available after %ds' % (loc.path, diff), end='\033[J\r')
                yield from asyncio.sleep(1)

            print('%s available, pushing' % loc.path, end='\r')
            yield from loc.path >> tag.add(**loc._asdict()) >> self.out
            time = loc.end


class Fetch(Unit, Paramed):
    """ fetch ressources in chunks """
    def __init__(self, root, **kws):
        super().__init__(**kws)
        self.root = root

    @param
    def chunksize(self, chunksize='16m'):
        return param.sizeof(chunksize)

    @outport
    def out(): pass

    @inport
    def ins(self, path, tag):
        reader = yield from self.root.open(path).reader()
        offset = 0
        while True:
            chunk = yield from reader.read(self.chunksize)
            yield from chunk >> tag.add(offset=offset) >> self.out
            offset += len(chunk)
            if not chunk:
                break


class Gunzip(Unit):
    """ unzip chunks of data """

    @coroutine
    def __setup__(self):
        self._decomp = None

    @outport
    def out(): pass

    @inport
    def ins(self, data, tag):
        if not self._decomp:
            decomp = self._decomp = zlib.decompressobj(zlib.MAX_WBITS|32)
        else:
            decomp = self._decomp

        if not data:
            tail = decomp.flush()
            self._decomp = None
            if tail:
                print('unprocessed raw data (maybe gz is corrputed!)')
                print(repr(tail))
            yield from '' >> tag >> self.out
        else:
            data = decomp.decompress(data).decode()
            yield from data >> tag >> self.out


class HTTPConnection:
    def __init__(self, base, connect={}, loop=None, **rqs):
        self.base = base
        self.connect = connect
        self.rqs = rqs

    @delayed
    def connector(self):
        return (self.rqs.pop('connector', None) 
                or aiohttp.TCPConnector(loop=asyncio.get_event_loop(), **self.connect))
    
    @local
    def methods(self):
        return {}

    @coroutine
    def request(self, method, path):
        url = '/'.join([self.base, path])
        return (yield from aiohttp.request(method, url, connector=self.connector, **self.rqs))

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError

        try:
            return self.methods[name]
        except KeyError:
            request = self.request
            method = name.upper()
            @coroutine
            @wraps(request)
            def rq(path):
                return (yield from request(method, path))
            self.methods[name] = rq
            return rq


class HTTPRessource(Ressource):
    def __init__(self, path, conn):
        super().__init__(path)
        self.conn = conn

    @property
    @coroutine
    def stat(self):
        r = yield from self.conn.head(self.path)
        if r.status != 200:
            return None
        h = r.headers

        return Stats(self.path, self.path.endswith('/'), pd.Timestamp(h['last-modified']), int(h['content-length']))

    @coroutine
    def text(self, encoding=None):
        r = yield from self.conn.get(self.path)
        return (yield from r.text(encoding=encoding))

    @coroutine
    def bytes(self):
        r = yield from self.conn.get(self.path)
        return (yield from r.read())

    @coroutine
    def reader(self):
        r = yield from self.conn.get(self.path)
        return r.content


class HTTPDirectory(HTTPRessource, Directory):
    def __init__(self, path, conn, columns=None, **rqs):
        super().__init__(path, conn=conn)
        self.columns = columns
        self.rqs = rqs

    def extract_stat(self, stat):
        return Stats(stat['name'], stat['dir'], pd.Timestamp(stat['modified']), stat['size'])

    @coroutine
    def stats(self):
        """ stats (name, dir, modified, size) for ressources in directory """
        html = yield from self.text()
        tbl, = pd.read_html(html, **self.rqs)
        if self.columns:
            if isinstance(self.columns, dict):
                tbl = tbl.rename(columns=self.columns)
            else:
                tbl.columns=self.columns

        if 'dir' not in tbl:
            tbl['dir'] = tbl['name'].str.endswith('/')
        if tbl['size'].dtype == object:
            tbl['size'] = tbl['size'].apply(strtosize)

        return list(tbl.apply(self.extract_stat, axis=1).values)

    def open(self, name: str):
        """ return Ressource object for a ressource in this directory """
        return HTTPRessource('/'.join([self.path, name]), conn=self.conn)

    def go(self, name: str):
        """ get a subdirectory """
        return HTTPDirectory('/'.join([self.path, name]), conn=self.conn, columns=self.columns, **self.rqs)
