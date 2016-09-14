from ...core import asyncio, coroutine, Unit, inport, outport
from ...ext.params import Paramed, param

from pyadds.logging import log

import pandas as pd
import zlib
import time


@log
class Watch(Paramed, Unit):
    """watch a ressource directory for located resources becomming available """
    def __init__(self, accesses, **kws):
        super().__init__(**kws)
        self.accesses = accesses

    @outport
    def out(): pass

    @param
    def stable(self, value='30s'):
        return pd.datetools.to_offset(value)

    @param
    def skip_after(self, value='5min'):
        return pd.datetools.to_offset(value)

    @param
    def skip_num(self, value=720):
        return value

    @param
    def num_try(self, value=3):
        return value

    @inport
    def process(self, start, tag):
        tz = tag.tz
        start = orig = start or tag.start or '1h'
        try:
            start = pd.Timestamp('now', tz=tz) - pd.datetools.to_offset(start)
        except ValueError:
            start = pd.Timestamp(start, tz=tz)

        if tag.reset:
            if tag.reset is True:
                try:
                    rel = pd.datetools.to_offset(orig)
                except ValueError:
                    try:
                        rel = pd.datetools.to_offset(tag.end)
                    except ValueError:
                        rel = None
                rel = rel or pd.datetools.Second()
                rel = pd.datetools.to_offset(rel.rule_code)
            else:
                rel = pd.datetools.to_offset(tag.on)
            val = rel.nanos
            off = start.utcoffset() or pd.datetools.timedelta(0)
            start = pd.Timestamp((start + off).value
                                 // val * val, tz=start.tz) - off

        tz = start.tz
        time = pd.Timestamp(start, tz=tz)

        try:
            end = start + pd.datetools.to_offset(tag.end)
        except (TypeError, ValueError):
            end = pd.Timestamp(tag.end, tz=tz)

        if not pd.isnull(end) and tag.on:
            rel = pd.datetools.to_offset(tag.on).nanos
            end = pd.Timestamp(end.value // rel * rel, tz=end.tz)

        self.__log.info('fetching from %s to %s', start,
                        '...' if pd.isnull(end) else end)

        last = None
        before = None
        accesses = self.accesses

        while not time >= pd.Timestamp(end, tz=time.tz):
            avails = [a.available(time, **tag) for a in accesses]
            avail = min(avails)

            now = pd.Timestamp('now', tz=avail.tz)
            wait = (avail-now).total_seconds()
            if wait > 0:
                self.__log.info('wating %ds for first access [%s]', wait, time)
                yield from asyncio.sleep(wait)
                now = pd.Timestamp('now', tz=avail.tz)

            stat = err = loc = None
            for n in range(self.num_try):
                try:
                    for avail, access in zip(avails, accesses):
                        if avail <= now:
                            s = (yield from access.stat(time, **tag))
                            self.__log.debug('%s: %s [%s]',
                                             access.name, s, time)
                            if s:
                                stat = s
                                if stat[1].begin == pd.Timestamp(time,
                                                                 tz=avail.tz):
                                    break

                    if stat:
                        access, loc, res = stat
                        self.__log.debug('%s-access is available [%s]',
                                         access.name, time)
                    else:
                        self.__log.debug('waiting for all (%d) accesses [%s]',
                                         len(accesses), time)
                        access = locl = res = None
                        first = None
                        skipping = set()
                        pending = {a.get(time, **tag) for a in accesses}
                        while pending:
                            done, pending = yield from asyncio.wait(
                                pending, return_when=asyncio.FIRST_COMPLETED)
                            first = first or next(iter(done))
                            self.__log('%s first of %s', first, done)

                            for d in done:
                                if d.exception():
                                    first = first or d
                                    continue
                                access, loc, res = d.result()
                                if loc.begin <= time:
                                    break
                                else:
                                    skipping.add((max(loc.begin, time),
                                                  access, loc, res))
                            else:
                                continue
                            break

                        for p in pending:
                            p.cancel()

                        if not access:
                            for _, access, loc, res in sorted(skipping):
                                break
                            else:
                                self.__log.warning('throwing error from %s',
                                                   first)
                                first.result() # should throw an error
                                assert None, "one of the accesses has to be in done"

                        if res is None:
                            self.__log.info('ignoring some missing files')
                        else:
                            self.__log.debug('finished with %s-access (%d)',
                                            access.name, len(done))
                    break
                except OSError as e:
                    self.__log.warning('error when getting ressource (%d time)',
                                       n+1, exc_info=True)
                    err = e

            if not loc:
                raise err

            if loc.begin >= pd.Timestamp(end, tz=loc.begin.tz):
                time = loc.begin
                break

            if loc.end == pd.Timestamp(before, tz=loc.end.tz):
                self.__log.warning('seems we start too loop %s -> %s - %s',
                                   time, loc.begin, loc.end)

            stable = access.isstable(time, **tag)

            t = tag.add(access=access.name,
                        stable=stable,
                        time=pd.Timestamp(time, tz=loc.begin.tz),
                        **loc._asdict())
            if start:
                if loc.begin < pd.Timestamp(start, tz=loc.begin.tz):
                    t['skip_to_time'] = pd.Timestamp(start, tz=loc.begin.tz)
                else:
                    start = None

            if loc.end > pd.Timestamp(end, tz=loc.end.tz):
                t['end_at_time'] = pd.Timestamp(end, tz=loc.end.tz)

            if last != access.name:
                last = access.name
                self.__log.info('using %s-access for %s ...', access.name, time)

            yield from loc.path >> t >> self.out

            before = time
            time = loc.end


@log
class Reader(Paramed, Unit):
    """ fetch resources in chunks """
    def __init__(self, accesses, **kws):
        super().__init__(**kws)
        self.accesses = {a.name: a for a in accesses}

    @param
    def chunksize(self, chunksize='15m'):
        return param.sizeof(chunksize)

    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.last = None

    @inport
    def process(self, path, tag):
        name = tag.access
        if name != self.last:
            self.last = name
            self.__log.info('reading with %s-access (%s ...)', name, tag.path)

        self.__log.debug('fetch file %s (%s) with %s', path, tag.begin, name)

        access = self.accesses[name]
        resource = access.resource(path)
        rq = None
        try:
            reader, rq = (yield from resource.reader())
        except OSError:
            if rq:
                yield from rq.release()
            if access.ignore_errors:
                yield from (b''
                            >> tag.add(flush=True, offset=0, chunk=0, size=0)
                            >> self.out)
                return
            raise
        tag.update(access.tag)

        chunksize = self.chunksize
        offset = 0
        times = waits = chunks = conts = 0
        done = False
        eof_continued = 0

        next = None

        start = time.time()
        first = None
        next = None

        while not done:
            try:
                chunk = yield from (next or reader.read(chunksize))
            except OSError as e:
                if rq:
                    yield from rq.release()
                self.__log.warning('%s while reading %s:%d',
                                   e, path, offset, exc_info=True)
                next = None
                yield from asyncio.sleep(2)
                reader, rq = yield from resource.reader(offset=offset)
                chunk = yield from reader.read(chunksize)
            size = len(chunk)

            eof = reader.at_eof()
            next = None

            # read more data when it is fast enough
            finish = time.time() + .4
            while not eof and size < chunksize:
                timeout = finish - time.time()
                if timeout <= 0:
                    break
                next = asyncio.async(reader.read(chunksize))
                try:
                    new = yield from asyncio.wait_for(asyncio.shield(next),
                                                      timeout=timeout)
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
                except Exception as e:
                    assert not eof, 'eof while read was sheduled'
                    self.__log.warn("%s when reading data", e, exc_info=True)
                    times += 1
                    break

            # for unstable files, we're only done if we checked for long enought
            done = eof and (tag.stable or (not chunk and eof_continued > 4))
            if done:
                tag = tag.add(flush=True)

            # if we have data output it
            if chunk or done:
                if not done and eof_continued:
                    self.__log.debug('eof status changed after '
                                     '%d (+%d...) [%s:%d]',
                                     eof_continued, size, path, offset)
                    eof_continued = 0

                yield from (chunk
                            >> tag.add(size=size, offset=offset, chunk=chunks)
                            >> self.out)
                if not offset:
                    first = time.time()
                assert size == len(chunk), 'size is right'
                offset += size

            # eof but not sure if file is stable yet, request beyond current end
            if not done and eof:
                while eof_continued <= 4:
                    yield from asyncio.sleep(.1)
                    eof_continued += 1
                    waits += 1
                    try:
                        if rq:
                            yield from rq.release()
                        reader, rq = yield from resource.reader(offset=offset)
                        break
                    except OSError:
                        pass
                if eof_continued > 4:
                    yield from (b''
                                >> tag.add(flush=True, offset=offset,
                                           chunk=chunks, size=0)
                                >> self.out)
                    break

            chunks += 1

        end = time.time()

        if rq:
            yield from rq.release()
        self.__log.debug('fetch done %s (%s) [%3.1fs-%3.1fs|%d:%d:%d:%d|%d]',
                         path, tag.begin,
                         first-start, end-start,
                         chunks, waits, conts, times, offset)


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

    @outport
    def out(): pass

    @outport
    def tails(): pass

    @coroutine
    def flush(self, tag):
        if self.decomp:
            self.__log.debug('flushing gunzip')
            tail = self.decomp.flush()
            if tail:
                self.__log.warning('uncrompressed raw data: %s', repr(tail))
                yield from tail >> tag.add(flush=True) >> self.tails
            self.decomp = None

    @inport
    def process(self, raw, tag):
        if tag.offset == 0 and self.decomp:
            yield from self.flush(tag)

        if not self.decomp:
            decomp = self.decomp = zlib.decompressobj(zlib.MAX_WBITS | 32)
            self.offset = 0
        else:
            decomp = self.decomp

        data = decomp.decompress(raw)

        size = len(data)
        self.offset += size

        yield from data >> tag.add(offset=self.offset, size=size) >> self.out

        if tag.flush:
            yield from self.flush(tag)
