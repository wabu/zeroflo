from .util import WithPath
from .annotate import local
from . import zmqtools

import asyncio
import aiozmq
import weakref
import sys
from asyncio import coroutine

import logging
logger = logging.getLogger(__name__)

connectors = {}

def connector(cls):
    connectors[cls.kind] = cls()
    return cls

class PacketTracker:
    def __init__(self, path):
        self.path = path
        self.stream = None

    @property
    def address(self):
        return 'ipc://{}/packet-tracker'.format(self.path.namespace)

    @coroutine
    def connect(self):
        logger.debug('PKT.connect %s', self.address)
        self.stream = yield from zmqtools.create_zmq_stream(
                aiozmq.zmq.DEALER, connect=self.address)
        logger.debug('PKT.connected %s', self.address)

    @coroutine
    def aquire(self, num=1):
        yield from self.stream.write(num.to_bytes(4, sys.byteorder, signed=True))

    @coroutine
    def release(self, num=1):
        yield from self.stream.write((-num).to_bytes(4, sys.byteorder, signed=True))

    @coroutine
    def wait(self):
        while True:
            yield from self.event.wait()
            logger.info('PKT.wait set')
            yield from asyncio.sleep(.5)
            if self.event.is_set():
                break
            logger.warn('PKT.wait reset after recheck')

    @local
    def event(self):
        event = asyncio.Event()
        event.clear()
        return event

    @coroutine
    def startup(self):
        logger.info('PKT.startup %s', self.path)
        stream = yield from zmqtools.create_zmq_stream(
                aiozmq.zmq.DEALER, bind=self.address)
        loop = asyncio.async(self.loop(stream, self.event))
        logger.info('PKT.started %s', self.path)
        return loop

    @coroutine
    def loop(self, stream, event):
        try:
            count = 0
            logger.info('PKT.looping')
            while True:
                p, = yield from stream.read()
                if count == 0:
                    self.event.clear()
                num = int.from_bytes(p, 'little', signed=True)
                count += num
                logger.debug('PKT<loop %-d -> %d', num, count)
                if count == 0:
                    self.event.set()
        finally:
            stream.close()
        logger.info('PKT.loop-done %s', self.path)


class Chan:
    def __init__(self, path):
        self.path = path

    @coroutine
    def setup(self):
        pass

    __typstr__ = '<>'

    def __str__(self):
        return '{}{}'.format(self.__typstr__, self.path)

    def __repr__(self):
        return '{}{}'.format(self.__typstr__, repr(self.path))


class InChan(Chan):
    __typstr__ = '<<'

    @coroutine
    def pull(self):
        raise NotImplementedError
    

class OutChan(Chan):
    __typstr__ = '<<'
    @coroutine
    def push(self, pid, packet):
        raise NotImplementedError


@connector
class LocalConnector:
    kind = 'local'
    queues = weakref.WeakValueDictionary()

    def get_q(self, path):
        try:
            q = self.queues[path]
            return q
        except KeyError:
            q = asyncio.Queue(16)
            self.queues[path] = q
            return q

    def mk_out(self, link):
        logger.debug('LCL.mk-out %s', link)
        return LocalOut(self.get_q(link.sync), path=link.sync)

    def mk_in(self, link):
        logger.debug('LCL.mk-in %s', link)
        return LocalIn(self.get_q(link.sync), path=link.sync)

class Local:
    def __init__(self, q, **kws):
        super().__init__(**kws)
        self.q = q

class LocalOut(Local, OutChan):
    @coroutine
    def push(self, pid, packet):
        logger.debug('LCO:push %s >> %s (%s)', packet, pid, self.path)
        yield from self.q.put((pid, packet)) 

class LocalIn(Local, InChan):
    @coroutine
    def pull(self):
        return (yield from self.q.get())


@connector
class ZmqConnector:
    kind = 'distribute'

    def get_direction(self, link):
        """
        tgt:
            src -.
            src -- tgt
            src -`

        src:
                .- tgt
            src -- tgt
                `- tgt

        None:
            src -- tgt
            src -- tgt
            src -- tg
        """
        num_src = link.source.unit.space.replicate
        num_tgt = link.target.unit.space.replicate
        if num_src <= 1:
            return 'src'
        if num_tgt <= 1:
            return 'tgt'
        
        raise NotImplementedError

    def mk_out(self, link):
        out = ZmqOut(path=link.sync, direction=self.get_direction(link))
        return out

    def mk_in(self, link):
        ins = ZmqIn(path=link.sync, direction=self.get_direction(link))
        return ins

class Zmq:
    def __init__(self, *, direction, **kws):
        super().__init__(**kws)
        self.direction = direction

class ZmqOut(Zmq, OutChan):
    @coroutine
    def setup(self):
        how = 'bind' if self.direction == 'src' else 'connect'
        address = 'ipc://{}/chan'.format(self.path.namespace)

        self.stream = yield from zmqtools.create_zmq_stream(aiozmq.zmq.DEALER, 
                **{how: address})
        logger.info('ZQ>.setup %s=%s', how, self.path)

    @coroutine
    def push(self, pid, packet):
        yield from self.stream.push(pid, packet)

class ZmqIn(Zmq, InChan):
    @coroutine
    def setup(self):
        how = 'bind' if self.direction == 'tgt' else 'connect'
        address = 'ipc://{}/chan'.format(self.path.namespace)

        self.stream = yield from zmqtools.create_zmq_stream(aiozmq.zmq.DEALER, 
                **{how: address})
        logger.info('ZQ<.setup %s=%s', how, self.path)

    @coroutine
    def pull(self):
        pid, packet = yield from self.stream.pull()
        return pid, packet



@connector
class ZmqDistirbute(ZmqConnector):
    kind = 'load-balance'

    def mk_balance(self, link):
        return 

    def mk_in(self, link):
        wrk = ZmqWorker


class ZmqLoadBalance(Chan):
    @coroutine
    def setup(self):
        self.incomming = yield from zmqtools.create_zmq_stream(
                aiozmq.zmq.DEALER, bind='ipc://{}/chan-in'.format(self.path.namespace))
        self.outgoing = yield from zmqtools.create_zmq_stream(
                aiozmq.zmq.ROUTER, bind='ipc://{}/chan-in'.format(self.path.namespace))
        return asyncio.async(loop)

    @coroutine
    def loop(self):
        incomming = self.incomming.pull
        recv = self.outgoing.recv
        push = self.outoging.push
        while True:
            load = yield from incomming()
            worker, _ = yield from recv()
            asyncio.async(push(worker, *load, skip=1))


class ZmqWorker(Zmq, InChan):
    @coroutine
    def setup(self):
        how = 'bind' if self.direction == 'tgt' else 'connect'
        address = 'ipc://{}/chan'.format(self.path.namespace)

        self.stream = yield from zmqtools.create_zmq_stream(aiozmq.zmq.DEALER, 
                **{how: address})
        logger.info('ZQ<.setup %s=%s', how, self.path)

    @coroutine
    def pull(self):
        yield from self.socket.write(b'')
        pid, packet = yield from self.stream.pull()
        return pid, packet


