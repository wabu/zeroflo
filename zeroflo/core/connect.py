from .exec import connector
from .util import WithPath
from . import zmqtools

import asyncio
import aiozmq
import weakref
from asyncio import coroutine

import logging
logger = logging.getLogger(__name__)

class InChan:
    def __init__(self, path):
        self.path = path

    @coroutine
    def setup(self):
        pass

    @coroutine
    def pull(self):
        raise NotImplementedError

    def __str__(self):
        return '<<{}'.format(self.path)

    def __repr__(self):
        return '<<{}'.format(repr(self.path))
    

class OutChan:
    def __init__(self, path):
        self.path = path

    @coroutine
    def setup(self):
        pass

    @coroutine
    def push(self, pid, packet):
        raise NotImplementedError

    def __str__(self):
        return '>>{}'.format(self.path)

    def __repr__(self):
        return '>>{}'.format(repr(self.path))


@connector
class LocalConnector:
    kind = 'union'
    queues = weakref.WeakValueDictionary()

    def get_q(self, path):
        try:
            q = self.queues[path]
            return q
        except KeyError:
            q = asyncio.JoinableQueue(10)
            self.queues[path] = q
            return q

    def mk_out(self, path):
        logger.debug('LCL.mk-out %s', path)
        return LocalOut(self.get_q(path), path=path)

    def mk_in(self, path):
        logger.debug('LCL.mk-in %s', path)
        return LocalIn(self.get_q(path), path=path)

class LocalOut(OutChan):
    def __init__(self, q, **kws):
        super().__init__(**kws)
        self.q = q

    @coroutine
    def push(self, pid, packet):
        logger.debug('LCO:push %s >> %s (%s)', packet, pid, self.path)
        yield from self.q.put((pid, packet)) 

    @coroutine
    def flush(self):
        logger.debug('LCO:flush (%s)', self.path)
        yield from self.q.join()


class LocalIn(InChan):
    def __init__(self, q, **kws):
        super().__init__(**kws)
        self.q = q

    @coroutine
    def pull(self):
        return (yield from self.q.get())

    @coroutine
    def done(self):
        self.q.task_done()


@connector
class ZmqConnector:
    kind = 'dist'
    def mk_out(self, path):
        out = ZmqOut(path=path)
        return out

    def mk_in(self, path):
        ins = ZmqIn(path=path)
        return ins

class ZmqOut(OutChan):
    @coroutine
    def setup(self):
        address = 'ipc://{}/chan'.format(self.path.namespace)
        self.stream = yield from zmqtools.create_zmq_stream(
                aiozmq.zmq.DEALER, connect=address)
        self.done = asyncio.Event()
        self.done.set()
        self.count = 0
        return asyncio.async(self.loop())

    @coroutine
    def push(self, pid, packet):
        if self.count == 0:
            self.done.clear()
        self.count += 1
        logger.debug('ZMO:push count=%d (%s)', self.count, self.path)
        yield from self.stream.push(pid, packet)

    @coroutine
    def loop(self):
        stream = self.stream
        done = self.done
        while True:
            logger.debug('ZMO:loop count=%d (%s)', self.count, self.path)
            nil, = yield from self.stream.read()
            assert nil == b'', "nil should be empty bytes, was %r" % nil
            logger.debug('ZMO:loop got %s (%s)', nil, self.path)
            self.count -= 1
            assert self.count >= 0, "task count should be >=0, was %d" % self.count
            if self.count == 0:
                logger.debug('ZMO:loop done (%s)', self.path)
                done.set()

    @coroutine
    def flush(self):
        logger.debug('ZMO:flush (%s)', self.path)
        yield from self.done.wait()

    
class ZmqIn(InChan):
    @coroutine
    def setup(self):
        address = 'ipc://{}/chan'.format(self.path.namespace)
        self.stream = yield from zmqtools.create_zmq_stream(aiozmq.zmq.ROUTER, 
                bind=address)

    @coroutine
    def pull(self):
        self.done, pid, packet = yield from self.stream.pull(skip=1)
        logger.debug('ZMI:pull %s<<%s [%s] (%s)', pid, packet, self.done, 
                     self.path)
        return pid, packet

    @coroutine
    def done(self):
        logger.debug('ZMI:done [%s] (%s)', self.done, self.path)
        return (yield from self.stream.write(self.done, b''))

