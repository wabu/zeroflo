from ..core import asyncio, Unit, inport, outport, coroutine
from ..ext import param, Paramed
from .read.ressource import LocateByTime

from pathlib import Path
from tempfile import NamedTemporaryFile

import re

from contextlib import contextmanager

from pyadds.logging import log, logging
from pyadds.annotate import delayed


@log
class ListFiles(Paramed, Unit):
    @param
    def dirs(self, dirs=['.']):
        """ directories where to look for files """
        return [Path(d) for d in dirs]

    @outport
    def out(): pass

    @inport
    def process(self, matches, tag):
        """ find all matching files """
        if isinstance(matches, str):
            matches = [matches]

        done = set()
        for match in matches:
            processed = False
            for path in self.dirs:
                for file in sorted(path.glob(match)):
                    if file.name in done:
                        continue
                    self.__log.info('file %r matches %r' % (str(file), match))
                    processed = True
                    done.add(file.name)
                    yield from str(file) >> tag >> self.out
            if not processed:
                raise ValueError('no matching files found for %r' % match)


class Reader(Paramed, Unit):
    """ read chunks of lines """
    @outport
    def out(): pass

    @param
    def chunksize(self, chunksize='15m'):
        return param.sizeof(chunksize)

    @param
    def warmup(self, val=None):
        if val is None:
            return self.chunksize
        else:
            return val

    @param
    def offset(self, offset=0):
        return offset

    @inport
    def process(self, path, tag):
        skip = tag.skip
        limit = tag.limit

        size = 0
        offset = self.offset
        warmup = self.warmup
        chunksize = self.chunksize

        with (yield from self.open(path)) as file:
            eof = False
            while not eof:
                size = 0
                chunks = []
                while True:
                    chunk = (yield from file.read(chunksize))
                    size += len(chunk)
                    chunks.append(chunk)
                    eof = file.at_eof()
                    if eof or warmup or size > chunksize:
                        break

                if warmup:
                    warmup -= size
                    if warmup < 0:
                        warmup = 0

                if eof:
                    tag = tag.add(eof=True, flush=True)

                data = b''.join(chunks)

                tag = tag.add(path=path, completed=self.status, offset=offset,
                              size=size)
                offset += size

                if skip:
                    if size >= skip:
                        skip -= size
                        continue
                    else:
                        data = data[skip:]
                        skip = 0

                if limit:
                    if limit <= size:
                        data = data[:limit]
                        eof = tag.eof = True
                        tag.limited = True
                    else:
                        limit -= size

                if data:
                    yield from data >> tag >> self.out
                elif eof:
                    # ensure that eof gets send out
                    yield from b'' >> tag >> self.out


class PBzReader(Reader):
    @delayed
    def extract_status(self):
        return re.compile(rb'Completed:\s*(\d*)%')

    @coroutine
    def read_status(self, status):
        self.status = 0

        text = b''
        while not status.at_eof():
            ins = yield from status.read(1024)
            text += ins
            *lines, text = text.split(b'\r')
            for l in lines:
                m = self.extract_status.search(l)
                if m:
                    self.status = int(m.groups()[0])

    @coroutine
    def open(self, path):
        pbzip2 = yield from asyncio.create_subprocess_exec(
            'pbzip2', '-vcd', path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            limit=int(self.chunksize*2.2))
        if not pbzip2.stdout._transport:
            logging.getLogger('asyncio').warning(
                "pipe has not transport, so we're fixing this manually")
            tr = pbzip2._transport.get_pipe_transport(1)
            pbzip2.stdout.set_transport(tr)

        if not pbzip2.stderr._transport:
            tr = pbzip2._transport.get_pipe_transport(2)
            pbzip2.stderr.set_transport(tr)

        status = asyncio.async(self.read_status(pbzip2.stderr))

        @contextmanager
        def closing():
            try:
                yield pbzip2.stdout
            except:
                try:
                    pbzip2.kill()
                except ProcessLookupError:
                    pass
                asyncio.async(pbzip2.wait())
                raise
            finally:
                status.cancel()
                yield from status

        return closing()


@log
class Writer(Paramed, Unit):
    @param
    def output(self, val):
        if isinstance(val, str):
            val = LocateByTime(val, width='30s')
        return val

    @param
    def mktemp(self, val=True):
        return val

    @param
    def seperator(self, val=b'\n'):
        return val

    @coroutine
    def __setup__(self):
        self.handle = None
        self.last = None

    @coroutine
    def __teardown__(self):
        yield from self.close()

    @coroutine
    def close(self):
        if self._processing:
            self.__log.warning('shutdown arrived while unit is stil processing')
        if self.handle:
            self.__log.info('flushing %s', self.last)
            try:
                yield from self.handle.drain()
            except AssertionError:
                self.__log.info('got strange error for drain', exc_info=True)
            finally:
                self.handle.close()
                self.handle = None
                Path(self.temp).rename(self.last)
                self.last = None

    @coroutine
    def _open(self, output):
        if self.mktemp:
            path, name = output.rsplit('/', 1)
            prefix, suffix = name.rsplit('.', 1)
            with NamedTemporaryFile(suffix='.'+suffix, prefix=prefix+'-', dir=path,
                                    delete=True) as f:
                self.__log.info('created temprery file %s', f.name)
                self.temp = output = f.name

        return (yield from self.open(output))

    @inport
    def process(self, data, tag):
        self._processing = True
        path = self.output.location(**tag).path
        if path != self.last:
            yield from self.close()
            self.last = path

        if data:
            if not self.handle:
                self.__log.info('opening %s for output', path)
                self.last = path
                self.handle = yield from self._open(path)

            self.handle.write(data)
            if not data.endswith(self.seperator):
                self.handle.write(self.seperator)
            yield from self.handle.drain()
        self._processing = False


class PBzWriter(Writer):
    @param
    def limit(self, val='63m'):
        return param.sizeof(val)

    @coroutine
    def open(self, filename):
        pbzip2 = yield from asyncio.create_subprocess_shell(
            'pbzip2 -c > {}'.format(filename),
            stdin=asyncio.subprocess.PIPE, limit=self.limit)

        if not pbzip2.stdin._transport:
            logging.getLogger('asyncio').warning(
                "pipe has not transport, so we're fixing this manually")
            tr = pbzip2._transport.get_pipe_transport(0)
            pbzip2.stdin.set_transport(tr)

        return pbzip2.stdin
