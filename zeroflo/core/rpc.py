from asyncio import coroutine
import aiozmq
import asyncio
from collections import Counter

from .zmqtools import create_zmq_stream

from pyadds.forkbug import maybug
from pyadds.annotate import cached

import logging
logger = logging.getLogger(__name__)

class Remote:
    def __init__(self, obj, endpoint):
        self.obj = obj
        self.endpoint = endpoint
        self.address = 'ipc://{}/rpc'.format(endpoint.namespace())
        
    @coroutine
    def __remote__(self):
        with maybug(info=self.obj, namespace=self.endpoint.namespace()):
            self.stream = stream = yield from create_zmq_stream(
                    aiozmq.zmq.REP, bind=self.address)
            obj = self.obj

            while True:
                rq,args,kws = yield from stream.pull()
                logger.debug('remote call to %s' % rq)
                method = getattr(obj, rq)

                result = yield from method(*args, **kws)
                yield from stream.push(result)


    @coroutine
    def __setup__(self):
        self.stream = yield from create_zmq_stream(
                aiozmq.zmq.REQ, connect=self.address)


    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError
        getattr(self.obj, name)

        @coroutine
        def sending(*args, **kws):
            logger.debug('calling remote to %s' % name)
            yield from self.stream.push(name, args, kws)
            logger.debug('pulling answer')
            return (yield from self.stream.pull())
        setattr(self, name, sending)
        return sending
        

class Track:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.address = 'ipc://{}/track'.format(endpoint.namespace())

class Tracker(Track):
    @coroutine
    def setup(self):
        logger.debug('trk connect %s', self.address)
        self.stream = yield from create_zmq_stream(
                aiozmq.zmq.DEALER, connect=self.address)

    @coroutine
    def aquire(self, idd, n=1):
        logger.debug('trk ++%d %x', n, idd)
        yield from self.stream.push(idd, n)

    @coroutine
    def release(self, idd, n=1):
        logger.debug('trk --%d %x', n, idd)
        yield from self.stream.push(idd, -n)


class Master(Track):
    @coroutine
    def setup(self):
        self.counter = Counter()
        logger.debug('tma bind %s', self.address)
        self.stream = yield from create_zmq_stream(
                aiozmq.zmq.DEALER, bind=self.address)
        return asyncio.async(self.loop())

    @cached
    def change(self):
        condition = self.condition
        if logger.isEnabledFor(logging.DEBUG):
            counter = self.counter
            @coroutine
            def change(idd, n):
                counter[idd] += n
                assert (any(c>0 for c in counter.values()) or
                        all(c==0 for c in counter.values())), 'negative pkg counter'
                logger.debug('trm >%+d %x: %+3d %r', n, idd, self.counter[idd], self)
                with (yield from condition):
                    condition.notify_all()
        else:
            @coroutine
            def change(idd, n):
                counter[idd] += n
                with (yield from condition):
                    condition.notify_all()
        return change


    @coroutine
    def loop(self):
        pull = self.stream.pull
        counter = self.counter
        change = self.change
        while True:
            idd, n = yield from pull()
            yield from change(idd, n)

    @coroutine
    def aquire(self, idd, n=1):
        logger.debug('trk ++%d %x', n, idd)
        yield from self.change(idd, n)

    @coroutine
    def release(self, idd, n=1):
        logger.debug('trk --%d %x', n, idd)
        yield from self.change(idd, -n)

    def __getitem__(self, items):
        if not isinstance(items, tuple):
            items = (items,)

        counter = self.counter
        def predicate():
            return not any(counter[item] for item in items)
        return predicate

    @cached
    def condition(self):
        return asyncio.Condition()

    @coroutine
    def await(self, *items):
        pred = self[items]
        logger.debug('trm await %s // %r', ','.join('%x' % i for i in items), self)
        with (yield from self.condition):
            yield from self.condition.wait_for(pred)

    def __repr__(self):
        return '|'.join('%x:%+d' % (i,c) for i,c in self.counter.items())



