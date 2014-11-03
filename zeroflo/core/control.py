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
            self.__log.warning('just exiting, no cleanup ...')
            yield from asyncio.sleep(1)
            exit(0)
        asyncio.async(down())

@log
class Control:
    def __init__(self, *, ctx, tp):
        self.tp = tp
        self.path = str(self.tp.path)
        self.spawner = ctx.spawner

        self.procs = {}
        self.units = {}
        self.queued = []
        atexit.register(self.__del__)

    def queue(self, coro):
        self.queued.append(coro)

    def replay(self):
        @coroutine
        def replay():
            for coro in self.queued:
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
            return self.procs[space]
        except KeyError:
            self.__log.debug('spawning for {!r}'.format(space))

            @coroutine
            def init_remote(i=None):
                path = space.path
                if i is not None:
                    path += idd.Named('replicate', 'rep-'+str(i))

                remote = rpc.Remote(Process(tracker=self.tracker), endpoint=path)

                proc = yield from self.spawner.cospawn(remote.__remote__, __name__=str(space))
                with open(path.namespace()+'/pids', 'a') as f:
                    f.write('{}\n'.format(proc.pid))
                
                yield from remote.__setup__()

                yield from remote.setup()
                return remote

            if space.replicate:
                remotes = yield from asyncio.gather(*(
                            init_remote(i) for i in range(space.replicate)))
                remote = rpc.Multi(remotes)
            else:
                remote = yield from init_remote()

            self.procs[space] = remote
            return remote

    def __del__(self):
        atexit.unregister(self.__del__)

        if self.procs:
            self.__log.info('shuting down all processes')
            future = asyncio.async(asyncio.gather(*[remote.shutdown() 
                        for remote in self.procs.values()], return_exceptions=True))
            asyncio.get_event_loop().run_until_complete(future)

            self.procs.clear()
        os.system("rm -rf {!r}".format(self.path))
    
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
        deps = self.tp.dependencies(unit, kind='target')
        self.__log.debug('%s depends on %s', unit, deps)
        deps = [p.pid for p in deps]
        self.__log.debug('%s depends on %s', unit, ['%x' % p for p in deps])
        yield from self.local.tracker.await(*deps)


    @coroutine
    def close(self, unit):
        chan = yield from self.ensure(self.tp[unit].space)
        yield from chan.close(unit.id.idd)
