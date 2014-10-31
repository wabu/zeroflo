from zeroflo import *
from pyadds.logging import *

import re

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
        rest = self.rest


        *lines,rest = (rest+data).split(self.seperator)
        if tag.flush:
            if rest.strip():
                lines.append(rest)
            rest=self.empty

        yield from lines >> tag >> self.out
        self.rest = rest


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

