from .annotate import local
from . import forkbug

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
        if name.startswith('_'):
            raise AttributeError
        return self.get(name, None)

    def __repr__(self):
        return ', '.join('{}: {}'.format(k,v) for k,v in self.items())

    def __str__(self):
        return ','.join(map(str, self.keys()))


def caller(name, self, *args, **kws):
    return getattr(self, name)(*args, **kws)


class Executor:
    __connectors__ = {}

    @classmethod
    def __register__(cls, connector):
        logger.info('CTX.register %s for %s', connector, connector.kind)
        cls.__connectors__[connector.kind] = connector()

    @local
    def ins(self):
        return {}

    def resolve_in(self, link):
        try:
            return self.ins[link.sync]
        except KeyError:
            conn = self.__connectors__ [link.options['kind']]
            chan = conn.mk_in(link.sync)
            self.ins[link.sync] = chan
            return chan

    @local
    def outs(self):
        return {}

    def resolve_out(self, link):
        try:
            return self.outs[link.sync]
        except KeyError:
            conn = self.__connectors__ [link.options['kind']]
            chan = conn.mk_out(link.sync)
            self.outs[link.sync] = chan
            return chan

    def __init__(self, ctx):
        self.ctx = ctx
        self.top = ctx.top

    @coroutine
    def setup(self):
        logger.info('EXC.setup')
        starts = []
        local = []
        for space in self.top.spaces:
            if space.is_setup:
                continue
            if space.is_local:
                local.append(self.setup_space(space))
            else:
                starts.append(self.spawn_space(space))

        yield from gather(*starts)

        yield from gather(*local)

    @local
    def mp(self):
        return mp.get_context('spawn')

    @coroutine
    def spawn_space(self, space):
        logger.debug('EXC.spawn-space %r [%r]', space, self.ctx)
        proc = self.mp.Process(target=caller, args=('__entry__', self, space), 
                               name=space.id.long)
        logger.debug('EXC.spawn-proc %r: %r [%r]', space, proc, self.ctx)
        proc.start()
        logger.debug('EXC.spawn-done %r: %d [%r]', space, proc.pid, self.ctx)
        space.is_setup = True

    def __entry__(self, space):
        #print(self, sid)
        #space = self.ctx.top.get_space(sid)
        #with forkbug.maybug(space.path):
        self.ctx.setup()
        logger.info('EXC.spawn-entry %r [%r]', space, self.ctx)
        @coroutine
        def waiting():
            loops = yield from self.setup_space(space)
            yield from wait(loops)
        self.ctx.loop.run_until_complete(waiting())

    @coroutine
    def setup_space(self, space):
        logger.debug('EXC.setup-space %r [%r]', space, self.ctx)
        space.is_local = True
        # XXX call config setup function here

        tasks = []
        for link in space.incomming:
            logger.debug('EXC+setup-in %r [%r/%r]', link, self.ctx, space)
            tasks.append(self.setup_in(link))

        for link in space.outgoing:
            logger.debug('EXC+setup-out %r [%r/%r]', link, self.ctx, space)
            tasks.append(self.setup_out(link))

        futures = (yield from gather(*tasks, loop=self.ctx.loop))
        space.is_setup = True
        return list(filter(bool, futures))

    @coroutine
    def setup_out(self, link):
        logger.debug('EXC.setup-out %r [%r]', link, self.ctx)
        chan = self.resolve_out(link)
        up = yield from chan.setup()

        self.out_chans[link.source.id].append((link.target.id, chan))
        logger.debug('EXC.setup-out %r -> %s [%r@%x]', link, self.out_chans, self.ctx, id(self))
        return up

    @local
    def out_chans(self):
        # -> {Pid: [OutChan]}
        return defaultdict(list)

    @coroutine
    def handle(self, src, packet):
        chans = self.out_chans[src]
        logger.debug('EXC.handle pushes %d (%r | %s) [%r@%x]', len(chans), src, self.out_chans, self.ctx, id(self))
        yield from gather(*[chan.push(tgt, packet) for tgt, chan in chans])

    @coroutine
    def flush(self):
        logger.debug('FLO:flush [%r/%r]', self.ctx, self)
        flushs = [c.flush() for chans in self.out_chans.values() for _,c in chans]
        logger.debug('FLO!flush %d links [%r/%r]', len(flushs), self.ctx, self)
        yield from gather(*flushs)
        logger.info('FLO!flushed [%r/%r]', self.ctx, self)
            

    @coroutine
    def setup_in(self, link):
        logger.debug('EXC.setup-in %r [%r]', link, self.ctx)
        chan = self.resolve_in(link)
        up = yield from chan.setup()

        return self.register_in(chan)

    def register_in(self, chan):
        logger.info('EXC:register %s [%r]', chan, self.ctx)
        return async(self.dispatch(chan), loop=self.ctx.loop)

    @coroutine
    def dispatch(self, chan):
        pull = chan.pull
        done = chan.done
        hdls = {}

        logger.debug('EXC:dispatch %s [%r]', chan, self.ctx)
        while True:
            pid,packet = yield from pull()
            try:
                hdl = hdls[pid]
            except KeyError:
                hdl = self.top.get_port(pid).handle

            with forkbug.maybug(pid.long):
                yield from hdl(packet)

            yield from done()


def connector(cls):
    Executor.__register__(cls)
    return cls

