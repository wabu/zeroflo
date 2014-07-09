from .annotate import local
from . import forkbug
from . import connect

from asyncio import coroutine, gather, wait, async
from collections import namedtuple, defaultdict

import operator
import multiprocessing as mp

import logging
logger = logging.getLogger(__name__)

class Packet(namedtuple('Packet', 'data tag')):
    """class for packets transfered inside flow"""
    def _data_len(self):
        try:
            return len(self.data)
        except:
            return None

    def _data_hash(self):
        try:
            return hash(self.data)
        except:
            return None

    def _data_str(self):
        l = self._data_len()
        h = self._data_hash()
        if l is None and hash is None:
            return None
        else:
            def ns(a): return '' if a is None else a
            return '{}:{}:{}'.format(
                    type(self.data).__name__, ns(l),ns(h))

    def __str__(self):
        ds = self._data_str()
        if ds is None:
            ds = '1:...'
        return "({}; {})".format(ds, str(self.tag))

    def __repr__(self):
        ds = self._data_str()
        if ds is None:
            ds = str(self.data).split('\n')[0]
            if len(ds) > 24:
                ds = ds[:21]+'...'
        return "({}; {})".format(ds, repr(self.tag))

    
class Tag(dict):
    """
    tag with metainfo about the packet
    >>> a = Tag(a=1, foo='baz')
    >>> b = a.add(b=2, foo='bar')
    >>> a.foo
        'baz'
    >>> a.b
    >>> b.foo
        'bar'
    >>> None >> Tag(b)
    """
    def add(self, **kws):
        new = Tag(self)
        new.update(kws)
        return new

    @classmethod
    def new(cls, **kws):
        return cls(kws)

    def __rrshift__(self, data):
        return Packet(data, self)

    def __getattr__(self, name):
        if name[0] == '_':
            raise AttributeError
        return self.get(name, None)

    def __repr__(self):
        return ', '.join('{}: {}'.format(k,v) for k,v in sorted(self.items()))

    def __str__(self):
        return ','.join(map(str, sorted(self.keys())))


def caller(name, self, *args, **kws):
    return getattr(self, name)(*args, **kws)


class Executor:
    def __init__(self, ctx):
        self.ctx = ctx
        self.top = ctx.top
        self.track = connect.PacketTracker(ctx.path)

    @coroutine
    def setup(self):
        logger.info('EXC.setup')
        starts = []
        local = []
        for space in self.top.spaces:
            if space.is_setup:
                continue
            if space.is_local:
                local.append(self.track.startup())
                local.append(self.setup_space(space))
            else:
                if space.replicate:
                    logger.debug('EXC.setup replicates %r %d [%r]', 
                            space, space.replicate, self.ctx)
                    starts.append(self.spawn('setup_repl', space))
                    starts.extend(self.spawn('setup_space', space) for i in range(space.replicate))
                else:
                    starts.append(self.spawn('setup_space', space))

            space.is_setup = True

        yield from gather(*starts)
        logger.info('EXC.setup-spawned %d [%r]', len(starts), self.ctx)
        yield from gather(*local)
        logger.info('EXC.setup-localed %d [%r]', len(local), self.ctx)

    @local
    def mp(self):
        return mp.get_context('spawn')

    def __entry__(self, coro, *args, **kws):
        self.ctx.setup()
        logger.info('EXC!spawn %s %s %s [%r]', coro, args, kws, self.ctx)

        coro = getattr(self, coro)
        @coroutine
        def waiting():
            loops = yield from coro(*args, **kws)
            if loops:
                yield from wait(loops)
        logger.info('EXC!spawn ioloop %s [%r]', coro, self.ctx)
        self.ctx.loop.run_until_complete(waiting())
        logger.info('EXC!spawn finished %s [%r]', coro, self.ctx)

    @coroutine
    def spawn(self, coro, *args, **kws):
        logger.debug('EXC.spawn %s %s %s [%r]', coro, args, kws, self.ctx)
        proc = self.mp.Process(target=caller, args=('__entry__', self, coro)+args, kwargs=kws)
        logger.debug('EXC.spawn-proc %r [%r]', proc, self.ctx)
        proc.start()
        logger.debug('EXC.spawn-done %d [%r]', proc.pid, self.ctx)

    @coroutine
    def setup_space(self, space):
        logger.debug('EXC.setup-space %r [%r]', space, self.ctx)
        # XXX call config setup function here
        space.is_local = True

        setups = []
        for unit in space.units:
            setups.append(forkbug.cowrapbug(unit.setup(), namespace=unit.path))
        yield from gather(*setups, loop=self.ctx.loop)

        tasks = []
        tasks.append(self.track.connect())

        for link in space.incomming:
            logger.debug('EXC+setup-in %r [%r/%r]', link, self.ctx, space)
            tasks.append(self.setup_in(link))

        for link in space.outgoing:
            logger.debug('EXC+setup-out %r [%r/%r]', link, self.ctx, space)
            tasks.append(self.setup_out(link))

        waits = yield from gather(*tasks, loop=self.ctx.loop)
        space.is_setup = True
        return [w for w in waits if w]

    @coroutine
    def setup_repl(self, space):
        tasks = []
        for link in space.incomming:
            if link.options['kind'] == 'replicate':
                tasks.append(self.resolve_repl(link))
        waits = yield from gather(*tasks, loop=self.ctx.loop)
        return [w for w in waits if w]

    @local
    def ins(self):
        return {}

    @coroutine
    def resolve_in(self, link):
        try:
            self.ins[link.sync]
        except KeyError:
            conn = connect.connectors[link.options['kind']]
            chan = conn.mk_in(link)
            self.ins[link.sync] = chan
            yield from chan.setup()
            return self.register_in(chan)

    @local
    def outs(self):
        return {}

    @coroutine
    def resolve_out(self, link):
        try:
            return self.outs[link.sync]
        except KeyError:
            conn = connect.connectors[link.options['kind']]
            chan = conn.mk_out(link)
            self.outs[link.sync] = chan
            yield from chan.setup()
            return chan

    @local
    def repls(self):
        return {}

    @coroutine
    def resolve_repl(self, link):
        try:
            self.repls[link.sync]
        except KeyError:
            conn = connect.connectors[link.options['kind']]
            repl = conn.mk_repl(link)
            self.repls[link.sync] = repl
            return (yield from repl.setup())

    @coroutine
    def setup_out(self, link):
        logger.debug('EXC.setup-out %r [%r]', link, self.ctx)
        chan = yield from self.resolve_out(link)
        self.out_chans[link.source.id].append((link.target.id, chan))
        logger.debug('EXC.setup-out %r -> %s [%r@%x]', 
                link, self.out_chans, self.ctx, id(self))

    @local
    def out_chans(self):
        #>> {Pid: [OutChan]}
        return defaultdict(list)

    @coroutine
    def handle(self, src, packet):
        chans = self.out_chans[src]

        yield from self.track.aquire(len(chans))
        yield from gather(*[chan.push(tgt, packet) for tgt, chan in chans])

    @coroutine
    def flush(self):
        logger.debug('FLO:flush [%r/%r]', self.ctx, self)
        yield from self.track.wait()
        logger.info('FLO!flushed [%r/%r]', self.ctx, self)

    @coroutine
    def setup_in(self, link):
        logger.debug('EXC.setup-in %r [%r]', link, self.ctx)
        return (yield from self.resolve_in(link))

    def register_in(self, chan):
        logger.info('EXC:register %s [%r]', chan, self.ctx)
        return async(self.dispatch(chan), loop=self.ctx.loop)

    @coroutine
    def dispatch(self, chan):
        pull = chan.pull
        release = self.track.release
        hdls = {}

        logger.info('EXC:dispatch %s [%r]', chan, self.ctx)
        while True:
            pid,packet = yield from pull()
            try:
                hdl = hdls[pid]
            except KeyError:
                hdl = self.top.get_port(pid).handle

            with forkbug.maybug(pid.long):
                yield from hdl(packet)

            yield from release()

