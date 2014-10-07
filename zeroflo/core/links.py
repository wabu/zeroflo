import weakref

import asyncio
import aiozmq
import zmq
from asyncio import coroutine

from pyadds.annotate import cached

from .zmqtools import create_zmq_stream


linkers = {}

def linker(kind):
    def annotate(cls):
        linkers[kind] = cls()
        return cls
    return annotate

class Chan:
    __show__ = '??'

    @coroutine
    def setup(self):
        pass

    @coroutine
    def close(self):
        pass

class InChan(Chan):
    __show__ = '<<'

    @coroutine
    def fetch(self):
        raise NotImplementedError

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
            self.queues[key] = q = asyncio.Queue(8)
            return q

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
    @cached
    def fetch(self):
        return self.queue.get

class LocalOut(OutChan, LocalChan):
    @cached
    def deliver(self):
        return self.queue.put


@linker(kind='par')
class ZmqLinker(Linker):
    @coroutine
    def mk_in(self, endpoint):
        return ZmqIn(endpoint)

    @coroutine
    def mk_out(self, endpoint):
        return ZmqOut(endpoint)


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

        stream = yield from create_zmq_stream(self.__stream_type__)
        stream.setsockopt(zmq.SNDHWM, 32)
        stream.setsockopt(zmq.RCVHWM, 32)
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


