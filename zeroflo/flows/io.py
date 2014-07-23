from ..core import asyncio, withloop, Unit, inport, outport, coroutine
from ..ext import param, Paramed

from pathlib import Path

import itertools as it
from contextlib import contextmanager

import logging
logger = logging.getLogger(__name__)

class ListFiles(Paramed, Unit):
    @param
    def dirs(self, dirs=['.']):
        """ directories where to look for files """
        return [Path(d) for d in dirs]

    @outport
    def out(): pass

    @inport
    def ins(self, matches, tag):
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
                    logger.info('file %r matches %r' % (str(file), match))
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
    def chunksize(self, chunksize='16m'):
        return param.sizeof(chunksize)

    @param
    def offset(self, offset=0):
        return offset

    @inport
    def ins(self, filename, tag):
        skip = tag.skip
        limit = tag.limit

        offset = self.offset
        lineno = 0
        rest = ''

        with (yield from self.open(filename)) as file:
            eof = False
            while not eof:
                chunk = (yield from file.read(self.chunksize)).decode()
                eof = file.at_eof()
                if eof:
                    tag = tag.add(eof=True, flush=True)
                tag = tag.add(filename=filename, lineno=lineno, offset=offset)

                first,*lines = chunk.split('\n')
                lines.insert(0, rest+first)

                rest = lines.pop()
                if eof and rest.strip():
                    lines.append(rest)

                length = len(lines)
                lineno += length
                offset += length

                if skip:
                    if length >= skip:
                        skip -= length
                        continue
                    else:
                        lines = lines[skip:]
                        skip = 0

                if limit:
                    if limit <= length:
                        lines = lines[:limit]
                        eof = tag.eof = True
                        tag.limited = True
                    else:
                        limit -= length

                if lines:
                    yield from lines >> tag >> self.out
                elif eof:
                    # ensure that eof gets send out
                    yield from [] >> tag >> self.out


class PBzReader(Reader):
    @coroutine
    @withloop
    def open(self, filename, loop=None):
        pbzip2 = yield from asyncio.create_subprocess_exec('pbzip2', '-cd', filename,
                stdout=asyncio.subprocess.PIPE, loop=loop)

        @contextmanager
        def closing():
            try:
                yield pbzip2.stdout
            except:
                pbzip2.kill()
                asyncio.async(pbzip2.wait(), loop=loop)
                raise
        return closing()
