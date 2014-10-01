from functools import wraps
from collections import defaultdict

import asyncio
from asyncio import coroutine

from pyadds.annotate import cached, delayed
from pyadds import spawn

from . import resolve
from . import rpc

import logging
logger = logging.getLogger(__name__)

class Process:
    def __init__(self, setup=None):
        self.receiver = resolve.Receiver.defaults()
        self.deliver = resolve.Deliver.defaults()
        self.outs = defaultdict(list)
        self.units = {}
        self.setup = setup

    @coroutine
    def register(self, unit, outs, ins):
        self.units[unit.tp.id.idd] = unit

        for l in ins:
            yield from self.receiver[l.endpoint].register(unit, l)

        for l in outs:
            chan = yield from self.deliver[l.endpoint].register(unit, l)
            self.outs[l.source.pid].append((l.target.pid, chan))
            
    @coroutine
    def activate(self, outs, ins):
        for l in ins:
            yield from self.receiver[l.endpoint].activate(l)

        for l in outs:
            yield from self.deliver[l.endpoint].activate(l)

            unit = self.units[l.source.unit.id.idd]
            l.source.of(unit).handle = self.handler(l.source.pid)

    @coroutine
    def ping(self, arg):
        print('remote says', arg)
        return 'pong', arg


    def handler(self, pid):
        outs = self.outs[pid] 
        @coroutine
        def handle(self, packet):
            yield from asyncio.gather(
                    *[chan.deliver((pid, packet)) for pid,chan in outs])
        return handle


    @coroutine
    def handle(self, port, packet):
        return (yield from port.of(self.units[port.unit.id.idd]).handle(*packet))


def setup_aiozmq():
    import asyncio
    import aiozmq
    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())


class Control:
    def __init__(self, *, ctx, tp):
        self.ctx = ctx
        self.tp = tp

        self.procs = {}
        self.units = {}

    def register(self, unit):
        self.units[unit.tp] = unit

    @cached
    def spawner(self):
        spawner = spawn.get_spawner('forkserver')
        spawner.add_setup(setup_aiozmq)
        spawner.add_setup(self.ctx.setup)
        return spawner


    @coroutine
    def ensure(self, space):
        try:
            return self.procs[space]
        except KeyError:
            logger.debug('spawning for {!r}'.format(space))
            remote = rpc.Remote(Process(), endpoint=space.path)

            yield from self.spawner.cospawn(remote.__remote__)
            yield from remote.__setup__()

            self.procs[space] = remote
            return remote

    
    @coroutine
    def activate(self, unit):
        u = self.tp.lookup(unit)
        logger.debug('activate {!r} on {!r}'.format(u, u.space))

        chan = yield from self.ensure(u.space)
        logger.debug('using {!r}'.format(chan))

        yield from chan.register(unit, self.tp.links_from(u), self.tp.links_to(u))
        logger.debug('registered')

        yield from chan.activate(self.tp.links_from(u), self.tp.links_to(u))
        logger.debug('activated')

        u.active = True
        for l in self.tp.links_from(u):
            tgt = l.target.unit
            if not tgt.active:
                yield from self.activate(self.units[tgt])


    @coroutine
    def handle(self, port, packet):
        chan = yield from self.ensure(port.tp.unit.space)
        yield from chan.handle(port.tp, packet)


def remote(f):
    streams = weakref.WeakKeyDictionry()

    @wraps(f)
    def bind(self, space):
        # get/create stream+process
        try:
            stream = streams[self]
        except KeyError:
            stream = mk_stream(space)

        @wraps(f)
        def send(self, *args, **kws):
            yield from self.stream.push(args, kws)
            return (yield from self.stream.pull())
        return send
    return bind
