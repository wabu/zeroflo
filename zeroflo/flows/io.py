from ..core import asyncio, Unit, inport, outport, coroutine
from ..ext import param, Paramed

from pathlib import Path

import itertools as it
from contextlib import contextmanager

from pyadds.logging import log, logging

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
    def process(self, filename, tag):
        skip = tag.skip
        limit = tag.limit

        size = 0
        offset = self.offset
        warmup = self.warmup
        chunksize = self.chunksize

        with (yield from self.open(filename)) as file:
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

                tag = tag.add(filename=filename, offset=offset, size=size)
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
    @coroutine
    def open(self, filename):
        pbzip2 = yield from asyncio.create_subprocess_exec('pbzip2', '-cd', filename,
                stdout=asyncio.subprocess.PIPE, limit=int(self.chunksize*2.2))
        if not pbzip2.stdout._transport:
            logging.getLogger('asyncio').warning(
                    "pipe has not transport, so we're fixing this manually")
            tr = pbzip2._transport.get_pipe_transport(1)
            pbzip2.stdout.set_transport(tr)

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
        return closing()


@log
class Writer(Paramed, Unit):
    @param
    def filename(self, val):
        return val

    @param
    def seperator(self, val=b'\n'):
        return val

    @coroutine
    def __setup__(self):
        self.f = yield from self.open(self.filename)

    @coroutine
    def __teardown__(self):
        self.f.close()
        yield from self.f.drain()

    @inport
    def process(self, data, tag):
        self.f.write(data+self.seperator)
        yield from self.f.drain()


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

