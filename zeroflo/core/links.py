import weakref

import asyncio
import aiozmq
import zmq
from asyncio import coroutine

from pyadds.annotate import cached
from pyadds import spawn
from pyadds.logging import log, logging

from .zmqtools import create_zmq_stream

linkers = {}

def linker(kind):
    def annotate(cls):
        linkers[kind] = cls()
        return cls
    return annotate

class Chan:
    __show__ = '??'

    def __init__(self, endpoint):
        self.endpoint = endpoint

    @coroutine
    def setup(self):
        pass

    @coroutine
    def close(self):
        pass

    def __str__(self):
        return '{}{}'.format(self.__show__, self.endpoint)

    def __repr__(self):
        return '{}{!r}'.format(self.__show__, self.endpoint)


class InChan(Chan):
    __show__ = '<<'

    @coroutine
    def fetch(self):
        raise NotImplementedError

    @coroutine
    def done(self):
        pass

class OutChan(Chan):
    __show__ = '>>'

    @coroutine
    def deliver(self, tgt, packet):
        raise NotImplementedError


class Linker:
    def mk(self, site):
        return {'target': self.mk_in,
                'source': self.mk_out}[site]

@linker(kind='local')
class LocalLinker(Linker):
    def __init__(self):
        self.queues = weakref.WeakValueDictionary()

    def get_q(self, key):
        try:
            return self.queues[key]
        except KeyError:
            return self.queues.setdefault(key, asyncio.JoinableQueue(1))

    @coroutine
    def mk_in(self, endpoint):
        return LocalIn(endpoint, self.get_q(endpoint))

    @coroutine
    def mk_out(self, endpoint):
        return LocalOut(endpoint, self.get_q(endpoint))


class LocalChan(Chan):
    def __init__(self, endpoint, q):
        self.endpoint = endpoint
        self.queue = q

    def __str__(self):
        return '{}{}'.format(self.__show__, self.endpoint)

    def __repr__(self):
        return '{}{}(!{:x})'.format(self.__show__, self.endpoint, id(self.queue))


class LocalIn(InChan, LocalChan):
    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)
        self._needed = False

    @coroutine
    def fetch(self):
        load = yield from self.queue.get()
        return load

    @coroutine
    def done(self):
        self.queue.task_done()

class LocalOut(OutChan, LocalChan):
    @coroutine
    def deliver(self, load):
        yield from self.queue.put(load)
        yield from self.queue.join()


@linker(kind='par')
class ZmqLinker(Linker):
    @coroutine
    def mk_in(self, endpoint):
        return ZmqIn(endpoint)

    @coroutine
    def mk_out(self, endpoint):
        return ZmqOut(endpoint)


@log
class ZmqChan(Chan):
    __stream_type__ = aiozmq.zmq.DEALER
    __stream_kind__ = None
    __stream_address__ = 'ipc://{}/chan'

    def __init__(self, endpoint):
        self.endpoint = endpoint

    @coroutine
    def setup(self):
        how = self.__stream_kind__
        addr = self.__stream_address__.format(self.endpoint.namespace())

        self.__log.info('setting up %s::%s', addr, how)

        stream = yield from create_zmq_stream(self.__stream_type__, limit=64*1024)
        stream.setsockopt(zmq.SNDHWM, 1)
        stream.setsockopt(zmq.RCVHWM, 1)
        stream.set_write_buffer_limits(64*1024)
        yield from getattr(stream, how)(addr)
        self.stream = stream
        return self

    def __str__(self):
        return '{}{}'.format(self.__show__, self.endpoint)

    def __repr__(self):
        return '{}{}(!{:x})'.format(self.__show__, self.endpoint, id(self.queue))


class ZmqIn(InChan, ZmqChan):
    __stream_kind__ = 'bind'

    @cached
    def fetch(self):
        return self.stream.pull


class ZmqOut(OutChan, ZmqChan):
    __stream_kind__ = 'connect'

    @cached
    def deliver(self):
        return self.stream.push



@linker(kind='repl')
class ZmqReplicate(ZmqLinker):
    @coroutine
    def mk_in(self, endpoint):
        return ZmqWorker(endpoint)

    @coroutine
    def mk_out(self, endpoint):
        return ZmqClient(endpoint)


@log
class ZmqClient(ZmqOut):
    __stream_address__ = 'ipc://{}/chan-in'
    proc = None

    @coroutine
    def setup(self):
        self.__log.debug('%s spawning zlb', self)
        lb = ZmqLoadBalance(self.endpoint)
        self.proc = spawn.get_spawner().spawn(lb.run)
        self.__log.debug('%s spawned zlb %s', self, self.proc)
        yield from super().setup()

    @coroutine
    def close(self):
        if self.proc:
            self.__log.info('killing zmq proc %s', self.proc)
            self.proc.terminate()
            del self.proc


@log
class ZmqWorker(ZmqIn):
    __stream_kind__ = 'connect'
    __stream_type__ = aiozmq.zmq.REQ
    __stream_address__ = 'ipc://{}/chan-out'

    @coroutine
    def fetch(self):
        yield from self.stream.write(b'')
        return (yield from self.stream.pull())


@log
class ZmqLoadBalance(Chan):
    __show__ = '<>'

    def run(self):
        logging.basicConfig(format='[%(process)d] %(name)s::%(levelname)5s %(message)s')
        self.__log.setLevel('DEBUG')

        try:
            context = zmq.Context()
            incomming = context.socket(zmq.DEALER)
            incomming.setsockopt(zmq.RCVHWM, 1)
            incomming.bind('ipc://{}/chan-in'.format(self.endpoint.namespace()))
            outgoing = context.socket(zmq.REP)
            outgoing.setsockopt(zmq.SNDHWM, 4)
            outgoing.bind('ipc://{}/chan-out'.format(self.endpoint.namespace()))
        except zmq.ZMQError as e:
            self.__log.info("failed to start %s", self, exc_info=True)
            return

        self.__log.info("running balancer %s", self)
        while True:
            #self.__log.debug('%s recving packet', self)
            #fpid = incomming.recv(copy=False)
            fpacket = incomming.recv(copy=False)
            #self.__log.debug('%s recving client %s', self, fpacket)
            _ = outgoing.recv()
            #self.__log.debug('%s sending packet to client', self)
            #outgoing.send(fpid, zmq.SNDMORE, copy=False)
            outgoing.send(fpacket, copy=False)

