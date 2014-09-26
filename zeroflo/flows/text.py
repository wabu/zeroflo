from zeroflo import *

import re

class Lines(Unit):
    @outport 
    def out(): pass

    @coroutine
    def __setup__(self):
        self.rest = ''

    @inport
    def ins(self, data, tag):
        rest = self.rest


        *lines,rest = (rest+data).split('\n')
        if tag.flush:
            if rest.strip():
                lines.append(rest)
            rest=''

        yield from lines >> tag >> self.out
        self.rest = rest


class Filter(Unit, Paramed):
    @param
    def pattern(self, value=None):
        if value is None:
            return
        return re.compile(value)

    @outport
    def out(): pass

    @inport
    def ins(self, lines, tag):
        yield from list(filter(self.pattern.search, lines)) >> tag >> self.out

