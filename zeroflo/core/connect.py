from .util import WithPath
from .annotate import local
from . import zmqtools

import asyncio
import aiozmq
import zmq
import weakref
import sys
from asyncio import coroutine

import logging
logger = logging.getLogger(__name__)

connectors = {}

def connector(cls):
    connectors[cls.kind] = cls()
    return cls


class Chan:
    def __init__(self, path):
        self.path = path

    @coroutine
    def setup(self):
        pass

    __typstr__ = '<>'
    __kind__ = None

    def __str__(self):
        return '{}{}'.format(self.__typstr__, self.path)

    def __repr__(self):
        return '{}{}'.format(self.__typstr__, repr(self.path))


class InChan(Chan):
    __typstr__ = '<<'
    __kind__ = 'in'

    @coroutine
    def pull(self):
        raise NotImplementedError
    

class OutChan(Chan):
    __typstr__ = '<<'
    __kind__ = 'out'

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
            q = asyncio.Queue(4)
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

    def mk_out(self, link):
        return ZmqOut(path=link.sync)

    def mk_in(self, link):
        return ZmqIn(path=link.sync)


class Zmq(Chan):
    __setup_type__ = aiozmq.zmq.DEALER
    __setup_kind__ = None
    __setup_address__ = 'ipc://{}/chan'

    @coroutine
    def setup(self):
        how = self.__setup_kind__
        addr = self.__setup_address__.format(self.path.namespace)
        self.stream = yield from zmqtools.create_zmq_stream(self.__setup_type__)
        self.stream.setsockopt(zmq.SNDHWM, 32)
        self.stream.setsockopt(zmq.RCVHWM, 32)
        yield from getattr(self.stream, how)(addr)
        logger.info('ZMQ.setup-%s %s=%s', self.__kind__, how, addr)

class ZmqOut(OutChan, Zmq):
    __setup_kind__ = 'connect'

    @coroutine
    def push(self, pid, packet):
        yield from self.stream.push(pid, packet)

class ZmqIn(InChan, Zmq):
    __setup_kind__ = 'bind'

    @coroutine
    def pull(self):
        pid, packet = yield from self.stream.pull()
        return pid, packet



@connector
class ZmqReplicate(ZmqConnector):
    kind = 'replicate'

    def mk_repl(self, link):
        return ZmqLoadBalance(link.sync)

    def mk_in(self, link):
        return ZmqWorker(link.sync)

    def mk_out(self, link):
        return ZmqClient(link.sync)


class ZmqClient(ZmqOut):
    __setup_address__ = 'ipc://{}/chan-in'

class ZmqWorker(InChan, Zmq):
    __typstr__ = '><'
    __setup_kind__ = 'connect'
    __setup_type__ = aiozmq.zmq.REQ
    __setup_address__ = 'ipc://{}/chan-out'

    @coroutine
    def pull(self):
        logger.debug('ZQW.pull %s', self)
        yield from self.stream.write(b'')
        logger.debug('ZQW.pull >>')
        pid, packet = yield from self.stream.pull()
        logger.debug('ZQW.pull %s << %s', pid, packet)
        return pid, packet

class ZmqLoadBalance(Chan):
    __kind__ = 'balance'

    def run(self):
        logger.info('ZQB.run %s', self)
        context = zmq.Context()
        incomming = context.socket(zmq.DEALER)
        incomming.setsockopt(zmq.RCVHWM, 128)
        incomming.bind('ipc://{}/chan-in'.format(self.path.namespace))
        outgoing = context.socket(zmq.REP)
        outgoing.setsockopt(zmq.SNDHWM, 512)
        outgoing.bind('ipc://{}/chan-out'.format(self.path.namespace))

        while True:
            fpid = incomming.recv(copy=False)
            fpacket = incomming.recv(copy=False)

            logger.debug('ZQB.loop incomming %r, %r', fpid, fpacket)

            _ = outgoing.recv()

            logger.debug('ZQB.loop outgoing')

            outgoing.send(fpid, zmq.SNDMORE, copy=False)
            outgoing.send(fpacket, copy=False)

            logger.debug('ZQB.loop send back task')

        logger.info('ZQB.done %s', self)
