from ...core import asyncio, coroutine, Unit, inport, outport
from pyadds.annotate import delayed
from ...ext.params import Paramed, param

from pyadds.logging import log

import pandas as pd
import zlib
import time

@log
class Watch(Paramed, Unit):
    """ watch a ressource directory for located resources becomming available """
    def __init__(self, root, locate, **kws):
        super().__init__(**kws)
        self.root = root
        self.locate = locate

    @outport
    def out(): pass

    @param
    def stable(self, value='30s'):
        return pd.datetools.to_offset(value)

    @param
    def skip_after(self, value='5min'):
        return pd.datetools.to_offset(value)

    @param
    def skip_num(self, value=240):
        return value

    @inport
    def process(self, start, tag):
        start = start or tag.start or '1h'
        try:
            start = pd.Timestamp('now', tz=tag.tz) - pd.datetools.to_offset(start)
        except ValueError:
            start = pd.Timestamp(start or tag.start, tz=tag.tz)

        tz = start.tz

        try:
            end = start + pd.datetools.to_offset(tag.end)
        except (TypeError, ValueError):
            end = pd.Timestamp(tag.end, tz=tz)

        self.__log.info('fetching from {start} to {end}'.format(
                    start=start, end=end or '...'))

        time = pd.Timestamp(start, tz=tz)
        locate = self.locate
        root = self.root

        while not time >= end:
            # wait until file may become available
            loc = locate.location(time)
            while True:
                wait = (loc.available - pd.Timestamp('now', tz=tz)).total_seconds()
                if wait <= 0:
                    break
                print('waiting %ds for %s' % (wait, loc.path), end='\033[J\r')
                yield from asyncio.sleep(1)

            # wait for file
            res = root.open(loc.path)
            while True:
                stat = yield from res.stat
                if stat:
                    break

                wait = (pd.Timestamp('now', tz=tz)-loc.available).total_seconds()
                if pd.Timestamp('now', tz=tz) > loc.available + self.skip_after:
                    skip_loc = loc
                    for k in range(1, self.skip_num+1):
                        skip_loc = locate.location(skip_loc.end)
                        skip_res = root.open(skip_loc.path)
                        stat = yield from skip_res.stat
                        if stat:
                            self.__log.warning('skipped %d to %s (%s)', 
                                    k, skip_loc.path, skip_loc.available)
                            res = skip_res
                            loc = skip_loc
                            break
                        if skip_loc.available > pd.Timestamp('now', tz=tz):
                            break
                if stat:
                    break

                print('awating %s for %ds' % (loc.path, wait), end='\033[J\r')
                yield from asyncio.sleep(min(max(.2, wait/20), 2))


            stable = (pd.Timestamp('now', tz=tz) - loc.available) > self.stable.delta

            yield from loc.path >> tag.add(stable=stable, **loc._asdict()) >> self.out
            time = loc.end


@log
class Fetch(Paramed, Unit):
    """ fetch resources in chunks """
    def __init__(self, root, **kws):
        super().__init__(**kws)
        self.root = root

    @param
    def chunksize(self, chunksize='15m'):
        return param.sizeof(chunksize)

    @outport
    def out(): pass

    @inport
    def process(self, path, tag):
        self.__log.debug('fetch file %s (%s)', path, tag.begin)
        resource = self.root.open(path)

        reader = yield from resource.reader()

        chunksize = self.chunksize
        offset = 0
        times = waits = chunks = conts = 0
        done = False
        eof_continued = 0

        next = None
        foo = False

        start = time.time()
        first = None
        next = None

        while not done:
            chunk = yield from (next or reader.read(chunksize))
            size = len(chunk)

            eof = reader.at_eof()
            next = None

            # read more data when it is fast enough
            finish = time.time() + .2
            while not eof and size < chunksize:
                timeout = finish - time.time()
                if timeout <= 0: 
                    break
                next = asyncio.async(reader.read(chunksize))
                try:
                    new = yield from asyncio.wait_for(asyncio.shield(next), timeout=timeout)
                    eof = reader.at_eof()
                    next = None
                    if not new:
                        break
                    conts += 1
                    chunk += new
                    size += len(new)
                except asyncio.TimeoutError:
                    assert not eof, 'eof while read was sheduled'
                    times += 1
                    break

            # for unstable files, we're only done if we checked for long enought
            done = eof and (tag.stable or (not chunk and eof_continued > 4))
            if done:
                tag = tag.add(flush=True)

            # if we have data output it
            if chunk or done:
                if not done and eof_continued:
                    self.__log.debug('eof status changed after %d (+%d...) [%s:%d]', 
                            eof_continued, size, path, offset)
                    eof_continued = 0

                yield from (chunk >> tag.add(size=size, offset=offset, chunk=chunks) 
                                  >> self.out)
                if not offset:
                    first = time.time()
                assert size == len(chunk), 'size is right'
                offset += size

            # eof but not sure if file is stable yet, we request beyond current end
            if not done and eof:
                while eof_continued <= 4:
                    yield from asyncio.sleep(.1)
                    eof_continued += 1
                    waits += 1
                    try:
                        reader = yield from resource.reader(offset=offset)
                        break
                    except OSError as e:
                        pass
                if eof_continued > 4:
                    yield from (b'' 
                            >> tag.add(flush=True, offset=offset, chunk=chunks, size=0) 
                            >> self.out)
                    break

            chunks += 1

        end = time.time()

        size = yield from resource.size
        self.__log.info('fetch done %s (%s) [%3.1fs-%3.1fs|%d:%d:%d:%d|%d/%d]', path, tag.begin, 
                        first-start, end-start, chunks, waits, conts, times, offset, size)
        assert size == offset, 'fetched data size different from size info'


class ListFiles(Paramed, Unit):
    @param
    def root(self, value):
        return value

    @outport
    def out(): pass

    @inport
    def process(self, pattern, tag):
        for match in (yield from self.root.glob(pattern)):
            yield from match >> tag >> self.out


@log
class Gunzip(Unit):
    """ unzip chunks of data """

    @coroutine
    def __setup__(self):
        self.decomp = None
        self.rest = None

    @outport
    def out(): pass

    @outport
    def chunk(): pass

    @coroutine
    def flush(self, tag):
        if self.decomp:
            self.__log.debug('flushing because of offset==0')
            tail = self.decomp.flush()
            if tail:
                self.__log.warning('uncrompressed raw data: %s', repr(tail))
            yield from tail >> tag.add(flush=True) >> self.chunk
            self.decomp = None

        if self.rest:
            rest = self.rest
            self.__log.debug('putting out rest of files (%d)', len(rest))
            yield from (rest >> tag.add(offset=self.offset, size=len(rest), flush=True)
                             >> self.out)

    @inport
    def process(self, raw, tag):
        if tag.offset == 0:
            yield from self.flush(tag)

        if not self.decomp:
            decomp = self.decomp = zlib.decompressobj(zlib.MAX_WBITS|32)
            self.offset = 0
        else:
            decomp = self.decomp

        data = decomp.decompress(raw)
        rest = self.rest
        if rest:
            data = rest + data
            rest = None

        if not tag.flush:
            data,*rest = data.rsplit(b'\n', 1)
            if rest:
                rest = b'\n'.join(rest)
            else:
                rest = data
                out = b''

        self.rest = rest
        size = len(data)
        self.offset += size

        yield from data >> tag.add(offset=self.offset, size=size) >> self.out

        if tag.flush:
            yield from self.flush(tag)
                

