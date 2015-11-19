from zeroflo import *
from pyadds.logging import *
from collections import defaultdict

class Split(Unit):
    @outport
    def out(): pass

    @outport
    def ids(): pass

    @coroutine
    def __setup__(self):
        self.pkid = 0

    @inport
    def process(self, data, tag):
        pkid = self.pkid
        yield from asyncio.gather(data >> tag.add(pkid=pkid) >> self.out,
                                  pkid >> tag.new() >> self.ids)
        self.pkid = pkid + 1


@log
class Union(Paramed, Unit):
    @param
    def autoflush(self, value=True):
        return value

    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.pkids = []
        self.loads = defaultdict(list)

    @coroutine
    def put(self):
        while self.pkids and self.loads[self.pkids[0]]:
            pkid = self.pkids[0]
            loads = self.loads[pkid]
            while loads:
                data, tag = loads.pop(0)
                yield from data >> tag >> self.out
                if self.autoflush or tag.flush:
                    assert not loads, 'more after flush, check your topology'
                    self.pkids.pop(0)
                    self.loads.pop(pkid)

    @inport
    def ids(self, pkid, tag):
        self.pkids.append(pkid)

    @inport
    def process(self, data, tag):
        if data is not None:
            pkid = tag.pop('pkid')
            self.__log.debug('got data for %s -> %s | %s',
                             pkid,
                             self.pkids and self.pkids[0], self.loads.keys())
            self.loads[pkid].append(data >> tag)

        yield from self.put()
