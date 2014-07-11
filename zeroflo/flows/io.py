from ..core import asyncio, withloop, Unit, inport, outport, coroutine
from ..ext import param, Paramed

from pathlib import Path

import itertools as it
from contextlib import contextmanager

import logging
logger = logging.getLogger(__name__)

class ListFiles(Paramed, Unit):
    @param
    def dirs(self, dirs=[]):
        return [Path(d) for d in dirs]

    @outport
    def out(): pass

    @inport
    def ins(self, matches, tag):
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
    @outport
    def out(): pass

    @param
    def chunksize(self, chunksize='16m'):
        return param.sizeof(chunksize)

    @param
    def strip_end(self, strip_end=True):
        return strip_end

    @inport
    def ins(self, filename, tag):
        skip = tag.skip
        limit = tag.limit

        lineno = 0
        rest = ''
        # TODO use async version
        with (yield from self.open(filename)) as file:
            eof = False
            while not eof:
                chunk = (yield from file.read(self.chunksize)).decode()
                eof = file.at_eof()
                if eof:
                    tag = tag.add(eof=True)
                tag = tag.add(filename=filename, lineno=lineno)

                first,*lines = chunk.split('\n')
                lines.insert(0, rest+first)

                rest = lines.pop()
                if eof and (not self.strip_end or rest.strip()):
                    lines.append(rest)

                lineno += len(lines)

                if skip:
                    if len(lines) >= skip:
                        skip -= len(lines)
                        continue
                    else:
                        lines = lines[skip:]
                        skip = 0

                if limit:
                    if limit <= len(lines):
                        lines = lines[:limit]
                        eof = tag.eof = True
                        tag.limited = True
                    else:
                        limit -= len(lines)

                if lines:
                    yield from lines >> tag >> self.out
                elif eof:
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
