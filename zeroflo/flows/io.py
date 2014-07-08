from ..core import asyncio, withloop, Unit, inport, outport, coroutine
from ..ext import param

import itertools as it
from contextlib import contextmanager

class Reader(Unit):
    @outport
    def out(): pass

    @param
    def chunksize(self, chunksize='16m'):
        return param.sizeof(chunksize)

    @inport
    def ins(self, files, tag):
        if isinstance(files, str):
            files = [files]

        for filename in files:
            lineno = 0
            rest = ''
            # TODO use async version
            with (yield from self.open(filename)) as file:
                eof = False
                while not eof:
                    chunk = (yield from file.read(self.chunksize)).decode()
                    eof = file.at_eof()

                    if eof:
                        lines,last = chunk.split('\n'), ''
                        tag = tag.add(filename=filename, lineno=lineno, eof=True)
                    else:
                        *lines,last = chunk.split('\n')
                        tag = tag.add(filename=filename, lineno=lineno)

                    if lines:
                        first,*lines = lines
                        if rest or first:
                            lines = [rest+first]+lines
                        rest = last
                        yield from lines >> tag >> self.out
                        lineno += len(lines)

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
