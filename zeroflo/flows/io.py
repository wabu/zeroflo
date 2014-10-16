from ..core import asyncio, Unit, inport, outport, coroutine
from ..ext import param, Paramed

from pathlib import Path

import itertools as it
from contextlib import contextmanager

from pyadds.logging import log

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
    def offset(self, offset=0):
        return offset

    @inport
    def process(self, filename, tag):
        skip = tag.skip
        limit = tag.limit

        offset = self.offset
        rest = b''

        chunksize = self.chunksize
        with (yield from self.open(filename)) as file:
            eof = False
            while not eof:
                chunk = rest
                while not eof and len(chunk) < chunksize:
                    chunk += (yield from file.read(chunksize))
                    eof = file.at_eof()

                if eof:
                    tag = tag.add(eof=True, flush=True)

                data,rest = chunk.rsplit(b'\n', 1)
                if eof and rest.strip():
                    data = chunk

                size = len(data)
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
                    yield from '' >> tag >> self.out


class PBzReader(Reader):
    @coroutine
    def open(self, filename):
        pbzip2 = yield from asyncio.create_subprocess_exec('pbzip2', '-cd', filename,
                stdout=asyncio.subprocess.PIPE)

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
