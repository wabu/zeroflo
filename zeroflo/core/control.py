from functools import wraps
from collections import defaultdict

import asyncio
from asyncio import coroutine

from pyadds.annotate import cached, delayed
from pyadds import spawn

from . import resolve

def rpc(f):
    # TODO auto rpc foo
    return coroutine(f)

class Process:
    def __init__(self):
        self.receiver = resolve.Receiver.defaults()
        self.deliver = resolve.Deliver.defaults()
        self.outs = defaultdict(list)

    @rpc
    def register(self, unit, outs, ins):
        for l in ins:
            yield from self.receiver[l.endpoint].register(unit, l)

        for l in outs:
            chan = yield from self.deliver[l.endpoint].register(unit, l)
            self.outs[l.source.pid].append((l.target.pid, chan))
            
    @rpc
    def activate(self, unit, outs, ins):
        for l in ins:
            yield from self.receiver[l.endpoint].activate(l)

        for l in outs:
            yield from self.deliver[l.endpoint].activate(l)

    def handler(self, pid):
        outs = self.outs[pid] 
        @coroutine
        def handle(packet):
            yield from asyncio.gather(
                    *[chan.deliver((pid, packet)) for pid,chan in outs])
        return handle


class Control:
    def __init__(self, *, ctx, tp):
        self.ctx = ctx
        self.tp = tp

        self.procs = {}
        self.units = {}

    def register(self, unit):
        self.units[unit.tp] = unit


    @cached
    def local(self):
        raise ValueError('no process object found for this process')

    @cached
    def spawner(self):
        return spawn.get_spawner('spawn')

    @coroutine
    def ensure(self, space):
        space = None
        try:
            return self.procs[space]
        except KeyError:
            proc = self.procs[space] = Process()
            self.local = proc
            return proc

    
    @coroutine
    def activate(self, unit):
        u = self.tp.lookup(unit)
        print('activate {!r}'.format(u))

        chan = yield from self.ensure(u.space)

        yield from chan.register(unit, self.tp.links_from(u), self.tp.links_to(u))
        yield from chan.activate(unit, self.tp.links_from(u), self.tp.links_to(u))

        u.active = True
        for l in self.tp.links_from(u):
            tgt = l.target.unit
            if not tgt.active:
                yield from self.activate(self.units[tgt])



    @cached
    def handle(self):
        chans = ...
        @coroutine
        def handle(pid, packet):
            for chan in chans[pid]:
                chan.send(packet)
        return handle


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
