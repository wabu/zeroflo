from functools import wraps
from collections import defaultdict

import asyncio
from asyncio import coroutine, Task

from pyadds.annotate import cached, delayed

from . import resolve
from . import rpc

import logging
logger = logging.getLogger(__name__)


class Process:
    def __init__(self, tracker):
        self.tracker = tracker
        self.receiver = resolve.Receiver.defaults(tracker=tracker)
        self.deliver = resolve.Deliver.defaults(tracker=tracker)
        self.outs = defaultdict(list)
        self.units = {}

    @coroutine
    def setup(self):
        logger.debug('setting up %s', self.tracker)
        yield from self.tracker.setup()

    @coroutine
    def register(self, unit, outs, ins):
        if not unit.id.idd in self.units:
            yield from unit.__setup__()
            self.units[unit.id.idd] = unit

        for l in ins:
            yield from self.receiver[l.endpoint].register(unit, l)

        for l in outs:
            chan = yield from self.deliver[l.endpoint].register(unit, l)
            outs = self.outs[l.source.pid]
            out = (l.target.pid, chan)
            if out not in outs:
                outs.append(out)
            l.source.of(unit).handle = self.handler(l.source.pid)
            
    @coroutine
    def activate(self, outs, ins):
        for l in ins:
            yield from self.receiver[l.endpoint].activate(l)

        for l in outs:
            yield from self.deliver[l.endpoint].activate(l)

    @coroutine
    def info(self):
        return Task.all_tasks()

    def handler(self, src):
        outs = self.outs[src] 
        aquire = self.tracker.aquire
        @coroutine
        def handle(self, packet):
            #yield from aquire(src, len(outs))
            aq = asyncio.gather(*(aquire(tgt) for tgt,_ in outs))
            dl = asyncio.gather(*(chan.deliver((tgt, packet)) 
                        for tgt,chan in outs))
            yield from asyncio.gather(aq, dl)
        return handle

    @coroutine
    def close(self, uid):
        ...


class Control:
    def __init__(self, *, ctx, tp):
        self.tp = tp
        self.spawner = ctx.spawner

        self.procs = {}
        self.units = {}
        self.queued = []

    def queue(self, coro):
        self.queued.append(coro)

    def replay(self):
        @coroutine
        def replay():
            for coro in self.queued:
                yield from coro
        return asyncio.async(replay())

    @cached
    def local(self):
        return None

    def register(self, unit):
        self.units[unit.id] = unit

    @coroutine
    def ensure(self, space):
        if not space.bound:
            if not self.local:
                proc = self.local = Process(tracker=rpc.Master(self.tp.path))
                yield from proc.setup()
            else:
                proc = self.local

            logger.debug('{!r} is local'.format(space))
            return self.local

        try:
            return self.procs[space]
        except KeyError:
            logger.debug('spawning for {!r}'.format(space))
            remote = rpc.Remote(Process(tracker=rpc.Tracker(self.tp.path)), 
                                endpoint=space.path)

            yield from self.spawner.cospawn(remote.__remote__)
            yield from remote.__setup__()

            yield from remote.setup()

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
            yield from self.activate(self.units[tgt.id])

    @coroutine
    def await(self, unit):
        deps = self.tp.dependencies(unit, kind='target')
        logger.debug('%s depends on %s', unit, deps)
        deps = [p.pid for p in deps]
        logger.debug('%s depends on %s', unit, ['%x' % p for p in deps])
        yield from self.local.tracker.await(*deps)


    @coroutine
    def close(self, unit):
        chan = yield from self.ensure(self.tp[unit].space)
        yield from chan.close(unit.id.idd)
