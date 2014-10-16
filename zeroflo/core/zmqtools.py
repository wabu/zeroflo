"""
Asyncio ZMQ tools
-----------------

- `create_zmq_stream`/`ZmqStream`: 
  a high level coroutine based interface to zmq messaging
"""

import aiozmq
import zmq
import asyncio
from asyncio import coroutine

import pickle

from functools import wraps
from collections import namedtuple

import types
import os

import logging
logger = logging.LoggerAdapter(logging.getLogger(__name__),
                               extra={'short': 'zmq'})
    
@coroutine
def create_zmq_stream(zmq_type, *, connect=None, bind=None, limit=None):
    """
    create ZmqStream stream based on `aiozmq.create_zmq_connection`
    """
    if bind:
        info = (zmq_type, 'bind', bind)
    elif connect:
        info = (zmq_type, 'connect', connect)
    else:
        info = (zmq_type, 'socket','')

    logger.debug('create zmq stream %s (%s=%s)', *info)

    if limit is None:
        limit = 16

    pr = ZmqStreamProtocol(limit)
    tr,_ = yield from aiozmq.create_zmq_connection(lambda: pr, 
            zmq_type=zmq_type, connect=connect, bind=bind)

    pr._stream = ZmqStream(tr, pr, limit)
    logger.debug('created %s', pr._stream)
    return pr._stream


def fwd(name, iface=aiozmq.ZmqTransport):
    f = getattr(iface, name)
    @wraps(f)
    def fwd(self, *args, **kws):
        return getattr(self._tr, name)(*args, **kws)
    return fwd

class ZmqStream:
    """
    High level coroutine based interface to read/write multipart zmq messages.
    Use `create_zmq_stream` to get a ZmqStream
    """
    def __init__(self, transport, protocol, limit):
        self._tr = transport
        self._pr = protocol
        self._limit = limit
        self._paused = False

    get_extra_info = fwd('get_extra_info')
    getsockopt = fwd('getsockopt')
    setsockopt = fwd('setsockopt')
    set_write_buffer_limits = fwd('set_write_buffer_limits')
    get_write_buffer_size = fwd('get_write_buffer_size')

    bind = fwd('bind')
    unbind = fwd('unbind')
    bindings = fwd('bindings')

    connect = fwd('connect')
    disconnect = fwd('disconnect')
    connections = fwd('connections')

    subscribe = fwd('subscribe')
    unsubscribe = fwd('unsubscribe')
    subscriptions = fwd('subscriptions')

    abort = fwd('abort')

    @coroutine
    def close(self):
        self._tr.close()
        yield from self._pr._closed.wait()

    @coroutine
    def read(self):
        """read a multipart message"""
        logger.debug('%s reading', self)
        datas = yield from self._pr._reading.get()
        if isinstance(datas, Exception):
            raise datas
        logger.debug('%s read %s', self, [len(p) for p in datas])
        if self._paused and self._pr._reading.qsize() < self._limit:
            logger.debug('%s resumes read', self)
            self._paused = False
            self._tr.resume_reading()
        return datas
        
    @coroutine
    def write(self, *datas, **kws):
        """write a multipart message"""
        logger.debug('%s writes %s', self, [len(p) for p in datas])
        yield from self._pr._writing.wait()
        logger.debug('%s writing', self)
        self._tr.write(datas, **kws)

    @coroutine
    def pull(self, skip=0, extract=True):
        """pull pickled objects from the socket"""
        datas = yield from self.read()
        result = datas[:skip] + [pickle.loads(d) for d in datas[skip:]]
        if extract and len(result) == 1:
            return result[0]
        else:
            return result

    @coroutine
    def push(self, *datas, skip=0, **kws):
        """push pickled objects on the socket"""
        if skip:
            yield from self.write(*(list(datas[:skip]) + 
                                    [pickle.dumps(d) for d in datas[skip:]]), **kws)
        else:
            yield from self.write(*[pickle.dumps(d) for d in datas[skip:]], **kws)

    def __str__(self):
        binds = list(self.bindings())
        conns = list(self.connections())
        if conns or binds:
            return str((binds+conns)[0])
        else:
            return 'unbound zmq stream'

    def __repr__(self):
        binds = list(self.bindings())
        conns = list(self.connections())
        return ','.join(map(str, binds+conns))


class ZmqStreamProtocol(aiozmq.ZmqProtocol):
    def __init__(self, limit):
        self._reading = asyncio.Queue()
        self._writing = asyncio.Event()
        self._closed = asyncio.Event()
        self._limit = limit
        self._stream = None

    def connection_made(self, transport):
        logger.debug('%s made connection', self)
        self._writing.set()

    def connection_lost(self, exc):
        # signal error/close
        logger.debug('%s lost connection %s', self, exc)
        asyncio.async(self._reading.put(exc or []))
        if exc is None:
            self._closed.set()
        else:
            self._exception = exc
        ...

    def pause_writing(self):
        logger.debug('%s pauses write', self)
        self._writing.clear()

    def resume_writing(self):
        logger.debug('%s resumes write', self)
        self._writing.set()

    def msg_received(self, datas):
        logger.debug('%s received %s', self, [len(p) for p in datas])
        if self._reading.qsize()-1 >= self._limit:
            logger.debug('%s full, pausing read', self)
            self._stream._paused = True
            self._stream._tr.pause_reading()
        asyncio.async(self._reading.put(datas))

    def __str__(self):
        return str(self._stream)

    def __repr__(self):
        return repr(self._stream)
