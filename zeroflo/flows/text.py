from zeroflo import *
from pyadds.logging import *

import re

class RemoveNullBytes(Paramed, Unit):
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
        yield from data.replaces(self.null, self.replace) >> tag >> self.out

class Chunker(Paramed, Unit):
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
        self.buf.extend(lines)
        tot = len(self.buf)

        if not tag.flush:
            self.__log.debug('adding %d lines -> %d', new, tot)
        else:
            self.__log.debug('flushing out %d+%d lines', new, len(self.buf))
            lines,self.buf = self.buf, []
            yield from sorted(lines) >> tag >> self.out


class Filter(Paramed, Unit):
    @param
    def tags(self, val={}):
        return val

    @param
    def pattern(self, value=None):
        if value is None:
            return
        return re.compile(value)

    @outport
    def out(): pass

    @inport
    def process(self, lines, tag):
        filt = list(filter(self.pattern.search, lines))
        yield from filt >> tag.add(**self.tags) >> self.out


class Join(Paramed, Unit):
    @param
    def seperator(self, val='\n'):
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from self.seperator.join(data) >> tag >> self.out

