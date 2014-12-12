from functools import wraps
from collections import defaultdict

import asyncio
import atexit
import os
from asyncio import coroutine, Task

from pyadds.annotate import cached, delayed
from pyadds.logging import log

from . import resolve
from . import rpc
from . import idd

@log
class Process:
    def __init__(self, tracker):
        self.tracker = tracker
        self.receiver = resolve.Receiver.defaults(tracker=tracker)
        self.deliver = resolve.Deliver.defaults(tracker=tracker)
        self.outs = defaultdict(list)
        self.units = {}

    @coroutine
    def setup(self):
        self.__log.debug('setting up %s', self)
        yield from self.tracker.setup()

    @coroutine
    def register(self, unit, outs, ins):
        self.__log.debug('register %r/%x', unit, id(unit))
        try:
            unit = self.units[unit.id.idd]
        except KeyError:
            yield from unit.__setup__()
            self.units[unit.id.idd] = unit

        for l in outs:
            chan = yield from self.deliver[l.endpoint].register(unit, l)
            outs = self.outs[l.source.pid]
            out = (l.target.pid, chan)
            if out not in outs:
                outs.append(out)
                l.source.of(unit).handle = self.handler(l.source.pid)

        for l in ins:
            yield from self.receiver[l.endpoint].register(unit, l)

    @coroutine
    def activate(self, outs, ins):
        for l in outs:
            yield from self.deliver[l.endpoint].activate(l)

        for l in ins:
            yield from self.receiver[l.endpoint].activate(l)

    @coroutine
    def info(self):
        return Task.all_tasks()

    def handler(self, src):
        outs = self.outs[src]
        aquire = self.tracker.aquire
        @coroutine
        def handle(self, packet):
            aq = asyncio.gather(*(aquire(tgt) for tgt,_ in outs))
            dl = asyncio.gather(*(chan.deliver((tgt, packet))
                        for tgt,chan in outs))
            yield from asyncio.gather(aq, dl)
        return handle

    @coroutine
    def close(self, uid):
        ...

    @coroutine
    def shutdown(self):
        @coroutine
        def down():
            try:
                self.__log.debug('shutdown closes all channels')
                yield from asyncio.gather(*(
                        chan.close() for outs in self.outs.values() for _,chan in outs))
                self.__log.debug('tearing down units')
                yield from asyncio.gather(*(
                        unit.__teardown__() for unit in self.units.values()))
            except Exception as e:
                self.__log.error('%s occured when shutting down', e, exc_info=True)

            yield from asyncio.sleep(.2)
            self.__log.debug('exiting')
            asyncio.get_event_loop().stop()
        asyncio.async(down())

@log
class Control:
    def __init__(self, *, ctx, tp):
        self.tp = tp
        self.path = str(self.tp.path)
        self.spawner = ctx.spawner

        self.procs = {}
        self.remotes = {}
        self.units = {}
        self.queued = []
        atexit.register(self.shutdown)

    def queue(self, coro):
        self.queued.append(coro)

    def replay(self):
        @coroutine
        def replay():
            while self.queued:
                coro = self.queued.pop()
                yield from coro
        yield from replay()
        #return asyncio.async(replay())

    @cached
    def local(self):
        return None

    @cached
    def tracker(self):
        return rpc.Tracker(self.tp.path)

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

            self.__log.debug('{!r} is local'.format(space))
            return self.local

        try:
            return self.remotes[space]
        except KeyError:
            self.__log.debug('spawning for {!r}'.format(space))

            @coroutine
            def init_rpc(i=None):
                path = space.path
                if i is not None:
                    path += idd.Named('replicate', 'rep-'+str(i))

                remote = rpc.Remote(Process(tracker=self.tracker), endpoint=path)

                proc = yield from self.spawner.cospawn(remote.__remote__, __name__=str(space))
                with open(path.namespace()+'/pids', 'a') as f:
                    f.write('{}\n'.format(proc.pid))

                yield from remote.__setup__()

                yield from remote.setup()
                return remote,proc

            if space.replicate:
                rpcs = yield from asyncio.gather(*(
                            init_rpc(i) for i in range(space.replicate)))
                remotes = [r for r,_ in rpcs]
                procs   = [p for _,p in rpcs]
                remote = rpc.Multi(remotes)
            else:
                remote,proc = yield from init_rpc()
                procs = [proc]

            self.remotes[space] = remote
            self.procs[space] = procs
            return remote

    def shutdown(self):
        atexit.unregister(self.shutdown)
        if self.procs:
            self.__log.info('shuting down all processes')
            @coroutine
            def shutdown(remote, procs):
                try:
                    yield from asyncio.wait_for(remote.shutdown(), timeout=5)
                except asyncio.TimeoutError:
                    self.__log.warn("can't shutdown %s properly, killing it", set(procs))
                    for p in procs:
                        p.terminate()

            future = asyncio.gather(*[shutdown(remote, proc)
                        for remote,proc in zip(self.remotes.values(), self.procs.values())],
                        return_exceptions=True)

            asyncio.get_event_loop().run_until_complete(future)
            for r in future:
                if r:
                    self.__log.warn('%r when shuting down', r)

            self.procs.clear()
            self.remotes.clear()

        os.system("rm -rf {!r}".format(self.path))


    def __del__(self):
        self.shutdown()

    @coroutine
    def activate(self, unit, actives=set()):
        u = self.tp.lookup(unit)
        self.__log.debug('activate {!r} on {!r}'.format(u, u.space))

        chan = yield from self.ensure(u.space)
        self.__log.debug('using {!r}'.format(chan))

        yield from chan.register(unit, self.tp.links_from(u), self.tp.links_to(u))
        self.__log.debug('registered')

        yield from chan.activate(self.tp.links_from(u), self.tp.links_to(u))
        self.__log.debug('activated')

        u.active = True
        for l in self.tp.links_from(u):
            tgt = l.target.unit
            if tgt in actives:
                continue
            yield from self.activate(self.units[tgt.id], actives|{tgt})

    @coroutine
    def await(self, unit):
        yield from self.replay()

        deps = self.tp.dependencies(unit, kind='target')
        self.__log.debug('%s depends on %s', unit, deps)
        deps = [p.pid for p in deps]
        self.__log.debug('%s depends on %s', unit, ['%x' % p for p in deps])
        yield from self.local.tracker.await(*deps)


    @coroutine
    def close(self, unit):
        chan = yield from self.ensure(self.tp[unit].space)
        yield from chan.close(unit.id.idd)
