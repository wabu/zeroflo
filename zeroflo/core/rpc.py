from asyncio import coroutine
import aiozmq
import asyncio
from collections import Counter

from .zmqtools import create_zmq_stream

from pyadds.annotate import cached
from pyadds.logging import log, logging

@log(short='tkm', sign='.!')
class Remote:
    def __init__(self, obj, endpoint):
        self.obj = obj
        self.endpoint = endpoint
        self.address = 'ipc://{}/rpc'.format(endpoint.namespace())
        
    @coroutine
    def __remote__(self):
        self.stream = stream = yield from create_zmq_stream(
                aiozmq.zmq.REP, connect=self.address)
        obj = self.obj

        while True:
            rq,args,kws = yield from stream.pull()
            self.__log.debug('remote call to %s' % rq)
            method = getattr(obj, rq)

            result = yield from method(*args, **kws)
            yield from stream.push(result)


    @coroutine
    def __setup__(self):
        self.stream = yield from create_zmq_stream(
                aiozmq.zmq.REQ, bind=self.address)


    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError
        getattr(self.obj, name)

        @coroutine
        def sending(*args, **kws):
            self.__log.debug('calling remote to %s' % name)
            yield from self.stream.push(name, args, kws)
            self.__log.debug('pulling answer')
            return (yield from self.stream.pull())
        setattr(self, name, sending)
        return sending


class Multi:
    def __init__(self, objs):
        self.objs = objs

    def __getattr__(self, name):
        ms = [getattr(obj, name) for obj in self.objs]
        @coroutine
        def multi(*args, **kws):
            return (yield from asyncio.gather(*(m(*args, **kws) for m in ms)))
        return multi

class Track:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.address = 'ipc://{}/track'.format(endpoint.namespace())

@log(short='tkr', sign='.!')
class Tracker(Track):
    @coroutine
    def setup(self):
        self.__log.debug('tracker connect %s', self.address)
        self.stream = yield from create_zmq_stream(
                aiozmq.zmq.DEALER, connect=self.address)

    @coroutine
    def aquire(self, idd, n=1):
        self.__log.debug('. %+d to %x', n, idd)
        yield from self.stream.push(idd, n)

    @coroutine
    def release(self, idd, n=1):
        self.__log.debug('. %-d to %x', -n, idd)
        yield from self.stream.push(idd, -n)


@log(short='tkm', sign='!.')
class Master(Track):
    @coroutine
    def setup(self):
        self.counter = Counter()
        self.__log.debug('master bind %s', self.address)
        self.stream = yield from create_zmq_stream(
                aiozmq.zmq.DEALER, bind=self.address)
        return asyncio.async(self.loop())

    @cached
    def change(self):
        condition = self.condition
        counter = self.counter
        if self.__log.isEnabledFor(logging.DEBUG):
            @coroutine
            def change(idd, n):
                counter[idd] += n
                assert (any(c>0 for c in counter.values()) or
                        all(c==0 for c in counter.values())), 'negative pkg counter'
                self.__log.debug('! %+d to %x gets %+3d %r', n, idd, self.counter[idd], 
                        self)
                if not counter[idd]:
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
        self.__log.debug('. %+d to %x', n, idd)
        yield from self.change(idd, n)

    @coroutine
    def release(self, idd, n=1):
        self.__log.debug('. %+d to %x', -n, idd)
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
        self.__log.debug('await %s // %r', ','.join('%x' % i for i in items), self)
        with (yield from self.condition):
            yield from self.condition.wait_for(pred)

    def __repr__(self):
        return '|'.join('%x:%+d' % (i,c) for i,c in self.counter.items())



