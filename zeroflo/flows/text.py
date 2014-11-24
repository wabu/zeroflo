from zeroflo import *
from pyadds.logging import *

import re

class RemoveNullBytes(Unit):
    @param
    def null(self, val=b'\x00'):
        return val

    @param
    def replace(self, val=b''):
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from data.replace(self.null, self.replace) >> tag >> self.out

class Chunker(Unit):
    @param
    def seperator(self, val=b'\n'):
        return val

    @coroutine
    def __setup__(self):
        self.nil = self.rest = self.seperator[:0]

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        data = self.rest + data

        if not tag.flush:
            data,*rest = data.rsplit(self.seperator, 1)
            if rest:
                rest, = rest
            else:
                rest = data
                data = self.nil
        else:
            rest = self.nil

        self.rest = rest

        if data or tag.flush:
            yield from data >> tag >> self.out


class Decode(Unit):
    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from data.decode() >> tag >> self.out


class Lines(Paramed, Unit):
    @param
    def seperator(self, val='\n'):
        return val

    @outport 
    def out(): pass

    @coroutine
    def __setup__(self):
        self.empty = self.seperator[:0]
        self.rest = self.empty

    @inport
    def process(self, data, tag):
        lines = data.split(self.seperator)
        yield from lines >> tag >> self.out


class Fields(Paramed, Unit):
    @param
    def seperator(self, val='\t'):
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        sep = self.seperator
        yield from [l.split(sep) for l in data] >> tag >> self.out


@log
class Sort(Unit):
    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.buf = []

    @inport
    def process(self, lines, tag):
        new = len(lines)
        if not tag.flush:
            self.buf.extend(lines)
            tot = len(self.buf)
            self.__log.info('adding %d lines -> %d', new, tot)
            return

        if self.buf:
            self.__log.info('flushing out %d+%d lines', new, len(self.buf))
            self.buf.extend(lines)
            lines,self.buf = self.buf, []

        yield from sorted(lines) >> tag >> self.out


class Filter(Paramed, Unit):
    @param
    def pattern(self, value=None):
        if value is None:
            return
        return re.compile(value)

    @outport
    def out(): pass

    @inport
    def process(self, lines, tag):
        yield from list(filter(self.pattern.search, lines)) >> tag >> self.out


class Join(Paramed, Unit):
    @param
    def seperator(self, val='\n'):
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from self.seperator.join(data) >> tag >> self.out

