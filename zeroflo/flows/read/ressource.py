from pyadds.annotate import cached
from ...ext.params import param, Paramed

import pandas as pd

import asyncio
coroutine = asyncio.coroutine

from collections import namedtuple
from pathlib import Path

from pyadds.logging import log


class Stats(namedtuple('Stats', 'name, dir, modified, size')):

    """ simple ressource stats """

    def __str__(self):
        dct = {f: getattr(self, f) for f in self._fields}
        return '{name:<12} {typ} {modified} {size:12d}'.format(
            typ='d' if self.dir else 'f', **dct)

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
        """ stats (name, dir, modified, size) of this ressource of None if not
            existent """
        raise NotImplementedError

    @property
    @coroutine
    def size(self):
        """ ressource size """
        return (yield from self.stat).size

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
    def reader(self, offset=0):
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
        stats = []
        for n in (yield from self.listen()):
            stats.append((yield from self.open(n).stat))
        return stats

    @coroutine
    def glob(self, pattern, max_depth=0):
        @coroutine
        def glob(d, path=Path(''), depth=0):
            stats = yield from d.stats()

            match = []
            dirs = []
            for stat in stats:
                p = (path/stat.name)
                if p.match(pattern):
                    name = str(p)
                    match.append(name)
                if stat.dir:
                    dirs.append((d.go(stat.name), path/stat.name))

            if depth+1 != max_depth:
                for d in dirs:
                    match.extend((yield from glob(*d, depth=depth+1)))

            return match

        return (yield from glob(self))

    @coroutine
    def await(self, name, poll=1.0):
        """ wait until the give ressource is created in this directory """
        ressource = self.open(name)
        while not (yield from ressource.exists):
            yield from asyncio.sleep(poll)
        return ressource

    def open(self, name):
        """ return Ressource object for ressource in this directory """
        raise NotImplementedError

    def go(self, name):
        """ get a subdirectory """
        raise NotImplementedError

    def iter(self):
        for stat in (yield from self.stats):
            if stat.dir:
                yield self.go(stat.name)
            else:
                yield self.open(stat.name)


Location = namedtuple('Location', 'path, begin, end, available')


class LocateByTime():

    def __init__(self, format, *, width, delay='0s', tz=None, **kws):
        self.width = width = pd.datetools.to_offset(width)
        self.delay = pd.datetools.to_offset(delay)
        self.format = format
        kws['width'] = width
        kws['delay'] = delay
        self.kws = kws
        self.tz = tz

    @cached
    def convert(self):
        kws = self.kws
        format = self.format

        def conversion(val):
            if isinstance(val, str):
                # assume strftime string
                return (lambda t: t.strftime(val))
            if isinstance(val, pd.offsets.Tick):
                # by multiples of ticks since epoch
                nanos = val.nanos
                return (lambda t: t.value // nanos)
            if isinstance(val, int):
                # milli seconds
                nanos = int(val * 10e6)
                return (lambda t: t.value // nanos)
            if isinstance(val, float):
                # seconds
                nanos = int(val * 1e9)
                return (lambda t: t.value // nanos)
            raise ValueError("No conversion possible with %s" % type(val))
        args = {name: conversion(val) for name, val in kws.items()}

        def convert(time):
            return format.format(time=time,
                                 **{k: v(time) for k, v in args.items()})
        return convert

    def location(self, time='now'):
        time = pd.Timestamp(time, tz=self.tz)
        nanos = time.value // self.width.nanos * self.width.nanos
        start = pd.Timestamp(nanos, tz=time.tz)
        return Location(self.convert(start),
                        start,
                        start + self.width,
                        start + self.width + self.delay)


@log
class Access(Paramed):

    def __init__(self, name, root, locate, **opts):
        super().__init__(**opts)
        self.name = name
        self.root = root
        self.locate = locate

    @param
    def skip_after(self, value='5min'):
        return pd.datetools.to_offset(value)

    @param
    def skip_num(self, value=240):
        return value

    @param
    def stable(self, value='30s'):
        return pd.datetools.to_offset(value)

    @param
    def tag(self, value={}):
        return value

    def available(self, time):
        loc = self.locate.location(time)
        return loc.available

    @coroutine
    def stat(self, time):
        loc = self.locate.location(time)
        res = self.root.open(loc.path)
        if (yield from res.stat):
            return self, loc, res

    @coroutine
    def wait(self, time):
        loc = self.locate.location(time)
        avail = loc.available

        wait = (avail - pd.Timestamp('now', tz=avail.tz)).total_seconds()
        if wait > 0:
            yield from asyncio.sleep(wait)

        return loc

    @coroutine
    def get(self, time):
        loc = (yield from self.wait(time))
        avail = loc.available
        res = self.root.open(loc.path)

        self.__log.debug('start getting by %s', self.name)
        while True:
            stat = yield from res.stat
            if stat:
                break

            now = pd.Timestamp('now', tz=avail.tz)
            wait = (now - avail).total_seconds()

            if now > avail + self.skip_after:
                skip_loc = loc
                for k in range(1, self.skip_num+1):
                    skip_loc = self.locate.location(skip_loc.end)
                    skip_res = self.root.open(skip_loc.path)
                    skip_stat = yield from skip_res.stat
                    if stat:
                        self.__log.warning(
                            'skipped %d to %s (%s)',
                            k,
                            skip_loc.path,
                            skip_loc.available)
                        res = skip_res
                        loc = skip_loc
                        stat = skip_stat
                        break
                    if skip_loc.available > now:
                        break

                if stat:
                    break

            sleep = min(max(.2, wait/10), 4)
            self.__log.debug('sleeping %f by %s', sleep, self.name)
            yield from asyncio.sleep(sleep)

        self.__log.debug('done getting by %s', self.name)
        return self, loc, res

    def isstable(self, time):
        loc = self.locate.location(time)
        avail = loc.available
        return avail + self.stable < pd.Timestamp('now', tz=avail.tz)

    def resource(self, path):
        return self.root.open(path)


class UnionRessource(Ressource):

    """ unify multiple resources into single one """

    def __init__(self, items):
        self.items = items

    @coroutine
    def _first(self):
        @coroutine
        def maybe(item):
            return item if (yield from item.exists) else None

        pending = map(maybe, self.items)
        while pending:
            done, pending = yield from asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                item = task.result()
                if item:
                    break

        for task in pending:
            task.cancel()
        return item

    @property
    @coroutine
    def stat(self):
        first = yield from self._first()
        return (yield from first.stat) if first else None

    @coroutine
    def reader(self, offset=0):
        first = yield from self._first()
        return (yield from first.reader(offset=0))


class UnionDirectory(Directory, UnionRessource):

    """ unify multiple directories into single one """
    @coroutine
    def listen(self):
        lists = yield from asyncio.gather([item.listen()
                                           for item in self.items])
        done = set()
        return [done.add(f) or f for ls in lists for f in ls if f not in done]

    @coroutine
    def glob(self, pattern, max_depth=0):
        lists = yield from asyncio.gather([item.glob(pattern, max_depth)
                                           for item in self.items])
        done = set()
        return [done.add(f) or f for ls in lists for f in ls if f not in done]

    def open(self, name):
        return UnionRessource([item.open(name) for item in self.items])

    def go(self, name):
        return UnionDirectory([item.go(name) for item in self.items])


class LocalReader:
    def __init__(self, path, offset=None):
        self.fd = path.open(mode='rb', buffering=0)
        self.eof = False
        if offset:
            self.fd.seek(offset)

    @coroutine
    def read_chunk(self, n=102400):
        return self.fd.read(n)

    def at_eof(self):
        return self.eof

    @coroutine
    def read(self, n=-1):
        if n > 0:
            data = self.fd.read(n)
            if not bool(data):
                self.eof = True
                self.fd.close()
            return data
        else:
            raise NotImplementedError()


class LocalRessource(Ressource):

    """ resource accessing local files """

    def __init__(self, path, read=None, limit='64m'):
        self.path = Path(path)
        self.read = read
        self.limit = param.sizeof(limit)

    @property
    @coroutine
    def stat(self):
        try:
            path = self.path
            stat = path.stat()
            return Stats(
                path.name,
                path.is_dir(),
                pd.Timestamp(
                    stat.st_mtime,
                    unit='s',
                    tz=pd.datetools.dateutil.tz.tzlocal()),
                stat.st_size)
        except FileNotFoundError:
            return

    @coroutine
    def reader(self, offset=None):
        if not self.read:
            self.read = ['cat', ...]
        #    return LocalReader(self.path, offset=offset)

        cmd = [str(self.path) if arg == ... else arg for arg in self.read]
        proc = (yield from asyncio.create_subprocess_exec(*cmd,
                stdout=asyncio.subprocess.PIPE, limit=self.limit))
        if not proc.stdout._transport:
            tr = proc._transport.get_pipe_transport(1)
            proc.stdout.set_transport(tr)
        if offset:
            yield from proc.stdout.readexactly(offset)
        return proc.stdout


class LocalDirectory(LocalRessource, Directory):

    """ directory accessing local dirs """
    @coroutine
    def listen(self):
        return [p.name for p in self.path.iterdir()]

    def open(self, name):
        return LocalRessource(str(self.path / name), read=self.read)

    def go(self, name):
        return LocalDirectory(str(self.path / name))
