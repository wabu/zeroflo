from collections import defaultdict
from contextlib import contextmanager

import asyncio
from asyncio import coroutine

from .links import linkers
from ..compat import JoinableQueue

from pyadds.logging import log
from pyadds.forkbug import maybug
from pyadds.annotate import delayed


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


@log
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
        self.__log.debug('dispatch activates %s for %s::%s', 'chan', self.endpoint, kind)
        yield from chan.setup()

    @coroutine
    def close_chan(self, kind, chan):
        self.__log.debug('close %s for %s::%s', 'chan', self.endpoint, kind)
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


@log
class Receiver(Resolver):
    __site__ = 'target'

    def __init__(self, endpoint, tracker):
        super().__init__(endpoint, tracker)

        self.queue = JoinableQueue(1)
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
        self.__log.debug('receiver activates %s for %s::%s', 'chan', self.endpoint, kind)
        yield from super().activate_chan(kind, chan)
        if not self.loops:
            assert not self.main
            self.main = asyncio.async(self.run())
        self.loops[kind] = asyncio.async(self.loop(chan))

    @coroutine
    def close_chan(self, kind, chan):
        yield from super().close_chan(kind, chan)
        loop = yield from self.loops.pop(kind)
        loop.cancel()
        yield from asyncio.gather(loop)

        if not self.loops:
            yield from self.queue.put((None, None))
            yield from self.main
            self.main = None

    @coroutine
    def loop(self, chan):
        self.__log.debug('looping %s: %s', self.endpoint, chan)
        fetch = chan.fetch
        done = chan.done
        join = self.queue.join
        put = self.queue.put
        while True:
            packet = yield from fetch()
            yield from put(packet)
            yield from join()
            yield from done()

    @coroutine
    def run(self):
        self.__log.debug('running %s', self.endpoint)
        get = self.queue.get
        done = self.queue.task_done
        prts = self.portmap

        aquire = self.tracker.aquire
        release = self.tracker.release

        while True:
            tgt, packet = yield from get()
            with maybug(namespace=self.endpoint):
                yield from prts[tgt](*packet)
            yield from release(tgt)

            done()


class Deliver(Resolver):
    __site__ = 'source'

    @coroutine
    def register(self, unit, link):
        chan = yield from super().register(unit, link)

        port = link.source.of(unit)
        port.channels.append(chan)
        return chan



