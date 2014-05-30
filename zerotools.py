"""
Asyncio ZMQ tools
-----------------

- `create_zmq_stream`/`ZmqStream`: as high level coroutine based interface to zmq messaging
- `ZmqRemote`: dispatch methods on in different process using zmq
- `ZmqProcess`: spawn process and use `ZmqRemote` to access remote object
- `ZmqSpawn`: 
"""

import aiozmq
import zmq
import asyncio
from asyncio import coroutine

import multiprocessing as mp
import pickle

from functools import wraps
from collections import namedtuple
import types
import os

@coroutine
def create_zmq_stream(zmq_type, *, connect=None, bind=None, limit=None):
    """
    create ZmqStream stream based on `aiozmq.ZmqEventLoop.create_zmq_connection`
    """
    if bind:
        info = ('bind', bind)
    elif connect:
        info = ('connect', connect)
    else:
        info = ('socket',)

    if limit is None:
        limit = 16

    pr = ZmqStreamProtocol(limit)
    tr,_ = yield from asyncio.get_event_loop().create_zmq_connection(lambda: pr, 
            zmq_type=zmq_type, connect=connect, bind=bind)

    pr._stream = ZmqStream(tr, pr, limit)
    return pr._stream


def fwd(name):
    @wraps(getattr(aiozmq.ZmqTransport, name))
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
        datas = yield from self._pr._reading.get()
        if isinstance(datas, Exception):
            raise datas
        if self._paused and self._pr_reading.qsize() < self._limit:
            self._pased = False
            self._tr.resume_reading()
        return datas
        
    @coroutine
    def write(self, *datas):
        """write a multipart message"""
        yield from self._pr._writing.wait()
        self._tr.write(datas)

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
    def push(self, *datas, skip=0):
        """push pickled objects on the socket"""
        if skip:
            yield from self.write(list(datas[:skip]) + [pickle.dumps(d) for d in datas[skip:]])
        else:
            yield from self.write(*map(pickle.dumps, datas[skip:]))

    def __str__(self):
        binds = list(self.bindings())
        conns = list(self.connections())
        return str((binds+conns)[0])

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
        self._writing.set()

    def connection_lost(self, exc):
        # signal error/close
        asyncio.async(self._reading.put(exc or []))
        if exc is None:
            self._closed.set()
        else:
            self._exception = exc
        ...

    def pause_writing(self):
        self._writing.clear()

    def resume_writing(self):
        self._writing.set()

    def msg_received(self, datas):
        if self._reading.qsize()-1 >= self._limit:
            self._stream._paused = True
            self._stream._tr.pause_reading()
        asyncio.async(self._reading.put(datas))

    def __str__(self):
        return str(self._stream)

    def __repr__(self):
        return repr(self._stream)


Result = namedtuple('Result', 'value')

class Normal(Result):
    def __call__(self):
        return self.value

class Except(Result):
    def __call__(self):
        raise self.value


class remote:
    """
    Annotation to make a coroutine accessable from remote.

    It uses the coroutine `obj.__remote__(name, args, kws)` of the object to do
    the actual remote call.
    """
    def __new__(cls, f):
        self = super().__new__(cls)
        self.__init__(f)
        return wraps(f)(self)

    def __init__(self, f):
        self.name = f.__name__
        self.definition = coroutine(f)
        self._pre = None
        self._post = None

    def __get__(self, obj, objtype):
        if obj is None:
            return self

        if obj.dispatching:
            return types.MethodType(self.definition, obj)
        else:
            return types.MethodType(self, obj)

    @coroutine
    def __call__(self, obj, *args, **kws):
        if self._pre:
            args, kws = yield from self._pre(obj, args, kws)
        result = yield from obj.__remote__(self.name, args, kws)
        if self._post:
            return (yield from self._post(obj, result, args, kws))
        else:
            return result()

    def before(self, f):
        """
        Call method on interface before remote with same arguments
        """
        f = coroutine(f)
        @wraps(f)
        def pre(obj, args, kws):
            yield from f(obj, *args, **kws)
            return args, kws
        return self.prepare(pre)

    def prepare(self, f):
        """
        Transfrom the arguments for remote call.

        The method gets args tuple and kws dict and should return an updated version
        """
        if self._pre:
            raise ValueError('{} already has a pre-annotation'.format(self))
        self._pre = coroutine(f)
        return self

    def after(self, f):
        """
        Call method on interface after remote with same arguments
        """
        f = coroutine(f)
        @wraps(f)
        def post(obj, result, args, kws):
            yield from f(obj, *args, **kws)
            return result, args, kws
        return self.postprocess(post)

    def postprocess(self, f):
        """
        Further process result of remote call

        The method gets the Result object, the args list and an kws dict and 
        should return the extrected result.
        """
        if self._post:
            raise ValueError('{} already has a post-annotation'.format(self))
        self._post = coroutine(f)
        return self

    def __str__(self):
        return self.name

    def __repr__(self):
        return '@remote:{}' % self.name


class ZmqRemote:
    """
    Mixin to bridge method calls between an interfacing and an dispatching
    side via zmq.
    """
    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)
        self._recv = None
        self._send = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_recv'] = None
        state['_send'] = None
        return state

    def __setstate__(self, state):
        self.__dict__ = state

    @property
    def dispatching(self):
        """
        are we dispatching things, eg. are we on the remote side
        """
        return self._recv is not None

    @property
    def interfacing(self):
        """
        are we interfacing with remote object
        """
        return self._send is not None

    @coroutine
    def setup_dispatching(self, address):
        """
        setup the dispatching process
        """
        self._recv = yield from create_zmq_stream(zmq.REP, bind=address)
        return asyncio.async(self._dispatching())

    @coroutine
    def _dispatching(self):
        recv = self._recv
        while True:
            request = yield from recv.pull()
            if not request:
                break
            result = yield from self._dispatch(*request)
            yield from recv.push(result)

    @coroutine
    def _dispatch(self, name, args, kws):
        method = getattr(self, name)
        try: 
            result = yield from method(*args, **kws)
            return Normal(result)
        except Exception as e:
            return Except(e)

    @coroutine
    def setup_interfacing(self, address):
        """
        setup an interacing connection
        """
        self._recv = None
        self._send = yield from create_zmq_stream(zmq.REQ, connect=address)

    @coroutine
    def __remote__(self, name, args, kws):
        if not self.interfacing:
            if self.dispatching:
                raise ValueError("__remote__ called on not-interfacing side")
            else:
                raise ValueError("__remote__ called on uninitalized object")

        yield from self._send.push(name, args, kws)
        return (yield from self._send.pull())

    @remote
    def pid(self):
        """
        get the pid of the dispatching process
        """
        return os.getpid()

    @remote
    def ping(self):
        """
        play ping-ping with remote
        """
        return 'pong'

class ZmqProcess(ZmqRemote):
    """
    remote call to process via zmq messages
    """
    _pid = None

    def __del__(self):
        if self._pid:
            asyncio.async(self.close())

    @coroutine
    def setup(self, address):
        """
        setup the process and open connection to it
        """
        self._pid = self.__spawn__(address)
        yield from self.setup_interfacing(address)

    def __proc__(self, address):
        asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
        asyncio.get_event_loop().run_until_complete(self.__main__(address))

    @coroutine
    def __main__(self, address):
        looping = yield from self.setup_dispatching(address)
        yield from looping

    @remote
    def close(self):
        """
        close the remote process
        """
        asyncio.async(self._recv.close())

    @close.after
    def close(self):
        yield from self._send.close()

    def __spawn__(self, *args, **kws):
        #proc = mp.Process(target=self.__proc__, args=args, kwargs=kws)
        #proc.start()
        #return proc.pid
        return fork.spawn(self, *args, **kws)

class Forker:
    def setup(self):
        self.rq = mp.Queue()
        self.pids = mp.Queue()
        self.proc = mp.Process(target=self.__proc__)
        self.proc.start()
        
    def __proc__(self):
        rq,pids = self.rq, self.pids
        for target, args, kws in iter(rq.get, None):
            proc = mp.Process(target=target.__proc__, args=args, kwargs=kws)
            proc.start()
            pids.put(proc.pid)

    def spawn(self, target, *args, **kws):
        self.rq.put((target, args, kws))
        return self.pids.get()

fork = None
