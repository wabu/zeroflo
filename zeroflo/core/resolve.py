from collections import defaultdict

import asyncio
from asyncio import coroutine

from .links import linkers

import logging
logger = logging.getLogger(__name__)


class Defaults(dict):
    def __init__(self, mk, *args, **kws):
        super().__init__(*args, **kws)
        self.mk = mk

    def __getitem__(self, item):
        try: 
            return super().__getitem__(item)
        except KeyError:
            result = self[item] = self.mk(item)
            return result


class Resolver:
    __site__ = None

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.chans = {}
        self.actives = {}

    @classmethod
    def defaults(cls):
        return Defaults(cls)

    @coroutine
    def register(self, unit, link):
        assert link.endpoint == self.endpoint

        try:
            chan = self.chans[link.kind]
        except KeyError:
            mk = linkers[link.kind].mk(self.__site__)
            chan = yield from mk(link.endpoint)
            self.actives[link.kind] = set()
            self.chans[link.kind] = chan

        return chan

    @coroutine
    def activate_chan(self, kind, chan):
        logger.debug('dispatch activates %s for %s::%s', 'chan', self.endpoint, kind)
        yield from chan.setup()

    @coroutine
    def close_chan(self, kind, chan):
        logger.debug('close %s for %s::%s', 'chan', self.endpoint, kind)
        yield from chan.close()

    @coroutine
    def activate(self, link):
        if not self.actives[link.kind]:
            yield from self.activate_chan(link.kind, self.chans[link.kind])
        self.actives[link.kind].add(link)

    @coroutine
    def close(self, link):
        self.actives[link.kind].remove(link)
        if not self.actives[link.kind]:
            yield from self.close_chan(self.chans[link.kind])


class Receiver(Resolver):
    __site__ = 'target'

    def __init__(self, endpoint):
        super().__init__(endpoint)

        self.queue = asyncio.Queue(8)
        self.portmap = {}
        self.loops = {}
        self.main = None

    @coroutine
    def register(self, unit, link):
        chan = yield from super().register(unit, link)

        port = link.target.of(unit)
        self.portmap[port.tp.pid] = port.handle
        return chan

    @coroutine
    def activate_chan(self, kind, chan):
        logger.debug('receiver activates %s for %s::%s', 'chan', self.endpoint, kind)
        yield from super().activate_chan(kind, chan)
        if not self.loops:
            assert not self.main
            self.main = asyncio.async(self.run())
        self.loops[kind] = asyncio.async(self.loop(chan))

    @coroutine
    def close_chan(self, kind, chan):
        yield from super().close_chan(kind, chan)
        yield from self.loops.pop(kind)

        if not self.loops:
            self.queue.put((None, None))
            yield from self.main
            self.main = None
        
    @coroutine
    def loop(self, chan):
        logger.debug('looping %s: %s', self.endpoint, chan)
        fetch = chan.fetch
        put = self.queue.put
        while True:
            packet = yield from fetch()
            yield from put(packet)

    @coroutine
    def run(self):
        logger.debug('running %s', self.endpoint)
        get = self.queue.get
        prts = self.portmap
        while True:
            pid, packet = yield from get()
            yield from prts[pid](*packet)


class Deliver(Resolver):
    __site__ = 'source'

    @coroutine
    def register(self, unit, link):
        chan = yield from super().register(unit, link)
        
        port = link.source.of(unit)
        port.channels.append(chan)
        return chan



