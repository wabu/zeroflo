from zeroflo import *
from pyadds.logging import *

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
class Union(Unit):
    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.pkids = []
        self.loads = {}

    @coroutine
    def put(self):
        while self.pkids and self.pkids[0] in self.loads:
            pkid = self.pkids.pop(0)
            load = self.loads.pop(pkid)
            self.__log.debug('putting out %s', pkid)
            yield from load >> self.out

    @inport
    def ids(self, pkid, tag):
        self.pkids.append(pkid)

    @inport
    def process(self, data, tag):
        if data is not None:
            pkid = tag.pop('pkid')
            self.__log.debug('got data for %s -> %s | %s', pkid, 
                    self.pkids and self.pkids[0], self.loads.keys())
            self.loads[pkid] = data >> tag

        yield from self.put()
        
