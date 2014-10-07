from collections import defaultdict

import asyncio
from asyncio import coroutine

from .links import linkers

import logging
logger = logging.getLogger(__name__)


class Defaults(dict):
    def __init__(self, mk, *args, **kws):
        self.mk = mk
        self.args = args
        self.kws = kws

    def __getitem__(self, item):
        try: 
            return super().__getitem__(item)
        except KeyError:
            result = self[item] = self.mk(item, *self.args, **self.kws)
            return result


class Resolver:
    __site__ = None

    def __init__(self, endpoint, tracker):
        self.tracker = tracker
        self.endpoint = endpoint
        self.chans = {}
        self.actives = {}

    @classmethod
    def defaults(cls, *args, **kws):
        return Defaults(cls, *args, **kws)

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

    def __init__(self, endpoint, tracker):
        super().__init__(endpoint, tracker)

        self.queue = asyncio.Queue(8)
        self.portmap = {}
        self.loops = {}
        self.main = None

    @coroutine
    def register(self, unit, link):
        chan = yield from super().register(unit, link)

        port = link.target.of(unit)
        self.portmap[link.target.pid] = port.handle
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
        aquire = self.tracker.aquire
        release = self.tracker.release
        while True:
            tgt, packet = yield from get()
            yield from prts[tgt](*packet)
            yield from release(tgt)


class Deliver(Resolver):
    __site__ = 'source'

    @coroutine
    def register(self, unit, link):
        chan = yield from super().register(unit, link)
        
        port = link.source.of(unit)
        port.channels.append(chan)
        return chan



