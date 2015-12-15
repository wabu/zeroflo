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
        yield from data.replace(self.null, self.replace) >> tag >> self.out


class ReplaceLinefeeds(Unit):
    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from data.replace(b'\r\n', b'\r\r') >> tag >> self.out

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

    @param
    def limit(self, val=-1):
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        sep = self.seperator
        lim = self.limit
        yield from [l.split(sep, lim) for l in data] >> tag >> self.out


@log
class Sort(Unit):
    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.buf = []

    @inport
    def process(self, lines, tag):
        if tag.sorted:
            yield from lines >> tag >> self.out
            return

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


@log
class Matcher(Paramed, Unit):
    @param
    def tag(self, val='match'):
        return val

    @param
    def default(self, val=None):
        return val

    @param
    def write_empty(self, val=True):
        return val

    @param
    def one_only(self, val=False):
        return val

    @param
    def resolve(self, val='raise'):
        assert val in {'raise', 'most', 'default'}
        return val

    @param
    def rules(self, val):
        return {k: re.compile(pat) for k, pat in val.items()}

    @outport
    def out(): pass

    @outport
    def non(): pass

    @inport
    def process(self, lines, tag):
        outs = {k: [] for k in self.rules.keys()}
        outs[self.default] = []
        for line in lines:
            for k, pat in self.rules.items():
                if pat.search(line):
                    break
            else:
                k = self.default
            outs[k].append(line)

        if self.one_only:
            out = [k for k, ls in outs.items() if len(ls)]
            if len(out) > 1:
                if self.resolve == 'most':
                    _, out = max((len(v), o) for o, v in outs.items())
                    self.__log.warning('%d matches, but one_only given, '
                                       'using %s with most', len(out), out)
                elif self.resolve == 'default':
                    out == self.default
                    self.__log.warning('%d matches, but one_only given, '
                                       'using %s as default', len(out), out)
                else:
                    raise ValueError(
                        'only one thingy should match with only_once')
                out = [out]
            out = (out or [self.default])[0]
            ls = outs[out]
            yield from ls >> tag.add(**{self.tag: out}) >> self.out

        else:
            yield from asyncio.gather(*[
                (ls >> tag.add(**{self.tag: out})
                    >> (self.non if out is None else self.out))
                for out, ls in outs.items() if self.write_empty or ls])


class Join(Paramed, Unit):
    @param
    def seperator(self, val='\n'):
        return val

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from self.seperator.join(data) >> tag >> self.out
