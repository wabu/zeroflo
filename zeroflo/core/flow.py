from .util import *
from . import sugar
from . import context
from . import zmqtools

import os
import weakref
import asyncio
import aiozmq
from collections import namedtuple, defaultdict

import logging

logger = logging.getLogger(__name__)

coroutine = asyncio.coroutine

class Context(Fled):
    __name__ = 'flo'
    __master__ = True

    def __init__(self, *args, setup=None, **kws):
        super().__init__(*args, **kws)
        self.setups = []
        if setup:
            self.setups.append(setup)

    def setup(self):
        for setup in self.setups:
            setup()

    @local
    def loop(self):
        asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
        return asyncio.get_event_loop()

    @shared
    def top(self):
        return Topology(self)

    @shared
    def ctrl(self):
        return Control(self)

    def path(self, *args):
        return os.path.join(self.extend, *map(str, args))


class Topology:
    def __init__(self, ctx):
        self.ctx = ctx
        self.fls = {}
        self.unions = []
        self.dists = []
        self.outs = defaultdict(list)
        self.ins = defaultdict(list)

    def register(self, fl):
        logger.info('TOP:register %s [%r]', fl, self.ctx)
        self.fls[fl.id] = fl

    def lookup(self, fid):
        return self.fls[fid]

    def _check_restrictions(self, unions, dists):
        for ds in dists:
            for us in unions:
                du = ds.intersection(us)
                if len(du) > 1:
                    raise ValueError(
                        "Following units are requested to "
                        "be unified and distributed:\n%s" % {self.lookup_fl(fid) for fid in du})
        return self

    def unify(self, *fls):
        ids = {fl.id for fl in fls}
        logger.info('TOP:unify %s [%r]', ids, self.ctx)
        unions = []
        for us in self.unions:
            if us.intersection(ids):
                ids.update(us)
            else:
                unions.append(us)
        unions.append(ids)
        logger.debug('TOP.unions %s [%r]', unions, self.ctx)

        self._check_restrictions(unions, self.dists)
        self.unions = unions

    def distribute(self, *fls):
        ids = {fl.id for fl in fls}
        logger.info('TOP:distribute %s [%r]', ids, self.ctx)
        dists = []
        for ds in self.dists:
            inter = ds.intersection(ids) 
            if inter == ds:
                ds.update(ids)
            else:
                dists.append(ds)
        dists.append(ids)
        logger.debug('TOP.dists %s [%r]', dists, self.ctx)

        self._check_restrictions(self.unions, dists)
        self.dists = dists

    @shared
    def spaces(self):
        spaces = set()
        rest = set(self.fls.keys())

        def mk_space(fls):
            logger.debug('TOP:mk-space %s [%r]', fls, self.ctx)
            space = Space(fls, self.ctx)
            spaces.add(space)
            rest.difference_update(fls)

        for us in self.unions:
            mk_space(us)

        # XXX prefare more local spaces ...
        for ds in self.dists:
            for fid in ds:
                if fid in rest:
                    mk_space({fid})
            
        if rest:
            mk_space(rest)

        logger.info('TOP.spaces %s [%r]', '|'.join(map(str, spaces)), self.ctx)

        return spaces

    def get_space(self, fid):
        for space in self.spaces:
            if fid in space.fids:
                return space
        raise ValueError("no space for %s" % fid)

    def link(self, source, target, **opts):
        logger.info('TOP.link %s >> %s // %s [%r]', source, target, opts, self.ctx)
        self.outs[source.pid].append((target.pid, opts))
        self.ins[target.pid].append((source.pid, opts))

    def link_kind(self, source, target):
        pair = {source.pid.fid, target.pid.fid}

        srcs = source.fl.space.fids
        if pair.intersection(srcs) == pair:
            return 'union'

        tgts = target.fl.space.fids
        for ds in self.dists:
            sd = ds.intersection(srcs)
            td = ds.intersection(tgts)
            if sd and td:
                return 'dist'

        return 'union'

    def links_from(self, src):
        links = []
        for tgt, opts in self.outs[src]:
            source = src.lookup(self.ctx)
            target = tgt.lookup(self.ctx)
            kind = self.link_kind(source, target)
            link = Link(self.ctx, source, target, kind=kind, **opts)
            logger.debug('TOP:links-from %r [%r]', link, self.ctx)
            links.append(link)
        return links

    def links_to(self, tgt):
        links = []
        for src, opts in self.ins[tgt]:
            source = src.lookup(self.ctx)
            target = tgt.lookup(self.ctx)
            kind = self.link_kind(source, target)
            link = Link(self.ctx, source, target, kind=kind, **opts)
            logger.debug('TOP:links-to %r [%r]', link, self.ctx)
            links.append(link)
        return links


class Space(Fled):
    __name__ = 'space'
    def __init__(self, fids, ctx, name=None):
        super().__init__(name=name)
        self.ctx = ctx
        self.fids = fids

    @local
    def fls(self):
        return {self.ctx.top.lookup(fid) for fid in self.fids}

    @local
    def ins(self):
        ins = set()
        for fl in self.fls:
            for port in fl.inports:
                ins.update(port.links)
        return ins

    @local
    def outs(self):
        outs = set()
        for fl in self.fls:
            for port in fl.outports:
                outs.update(port.links)
        return outs

    @local
    def local(self):
        return False

    @shared
    def is_setup(self):
        return False

    def register(self, chan):
        logger.info('SPC:register %s [%r/%r]', chan, self.ctx, self)
        return asyncio.async(self.dispatch(chan), loop=self.ctx.loop)

    @coroutine
    def dispatch(self, chan):
        top = self.ctx.top
        ports = {}

        logger.debug('SPC:dispatch %s [%r/%r]', chan, self.ctx, self)
        while True:
            pid, packet = yield from chan.pull()
            try:
                port = ports[pid]
            except KeyError:
                port = pid.lookup(self.ctx)
                ports[pid] = port

            yield from port.handle(packet)

    @coroutine
    def setup(self):
        self.ctx.setup()

        ctrl = self.ctx.ctrl
        tasks = []
        for link in self.ins:
            logger.debug('SPC>resolve-in %s [%r/%r]', link, self.ctx, self)
            tasks.append(ctrl.resolve_in(link))

        for link in self.outs:
            logger.debug('SPC>resolve-out %s [%r/%r]', link, self.ctx, self)
            tasks.append(ctrl.resolve_out(link))

        logger.info('SPC!resolve %d channels [%r/%r]', len(tasks), self.ctx, self)
        return (yield from asyncio.gather(*tasks, loop=self.ctx.loop))

    def __entry__(self):
        asyncio.async(self.setup(), loop=self.ctx.loop)
        self.ctx.loop.run_forever()

    def __str__(self):
        return '{}{{{}}}'.format(super().__str__(),
                ','.join(str(id) for id in self.fids))

    def __repr__(self):
        return '{}{{{}}}'.format(super().__repr__(),
                ','.join(repr(id) for id in self.fids))


class Pid(namedtuple("Pid", "fid, port")):
    def lookup(self, ctx):
        fl = ctx.top.lookup(self.fid)
        return getattr(fl.unit, self.port)


class Link:
    def __init__(self, ctx, src, tgt, **opts):
        self.ctx = ctx
        self.src = src.pid
        self.tgt = tgt.pid
        self.options = opts

    @local
    def source(self):
        return self.src.lookup(self.ctx)

    @local
    def target(self):
        return self.tgt.lookup(self.ctx)

    @local
    def sync(self):
        sin,sout = self.target.sync
        logger.debug('LNK:sync %s,%s [%r/%s]', sin.__name__, sout.__name__, self.ctx, self)
        sync = (self.ctx.id,) + sin(self.target) + sout(self.source)
        logger.debug('LNK=sync %s', sync)
        return sync

    @local
    def address(self):
        return tuple(s.long for s in self.sync)

    @local
    def channel(self):
        return None

    def register(self, chan):
        logger.debug('LNK.register %s [%r/%s]', chan, self.ctx, self)
        self.channel = chan

    @coroutine
    def handle(self, packet):
        logger.debug('LNK.handle %s [%r/%s]', packet, self.ctx, self)
        yield from self.channel.push(self.tgt,  packet)
        logger.debug('LNK#pushed to %s [%r/%s]', self.channel, self.ctx, self)

    def __str__(self):
        return '%s >> %s' % (self.source, self.target)

    def __repr__(self):
        return '%s >> %s // %s' % (self.source, self.target, self.options)


class Unit(sugar.UnitSugar):
    @shared
    def __fl__(self):
        return Fl(self)

    def __str__(self):
        return str(self.__fl__)

    def __repr__(self):
        return repr(self.__fl__)


class Fl(Fled):
    def __init__(self, unit, *, name=None, ctx=None):
        super().__init__(name=name or type(unit).__name__)

        if ctx is None:
            ctx = context.get_current_context()
        self.ctx = ctx
        self.unit = unit

        ctx.top.register(self)

    @local
    def space(self):
        return self.ctx.top.get_space(self.id)

    @property
    def ports(self):
        ports = []

        cls = type(self.unit)
        for name in dir(cls):
            if not name.startswith('_'):
                attr = getattr(cls, name)
                if isinstance(attr, Unbound):
                    ports.append(attr.__get__(self.unit, cls))

        return ports

    @property
    def inports(self):
        return [p for p in self.ports if isinstance(p, InPort)]

    @property
    def outports(self):
        return [p for p in self.ports if isinstance(p, OutPort)]


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


class Syncs:
    @classmethod
    def any(cls, port):
        return ()

    @classmethod
    def space(cls, port):
        return cls.any(port) + (port.fl.space.id,)

    @classmethod
    def unit(cls, port):
        return cls.space(port) + (port.fl.id,)

    @classmethod
    def port(cls, port):
        return cls.unit(port) + (port.id,)


class Unbound(Annotate, RefDescr, Get):
    __port__ = None
    __sync__ = Syncs.unit, Syncs.any

    def __get__(self, obj, objtype):
        if obj is None:
            return self

        dct,key = self.lookup(obj)
        try:
            return dct[key]
        except KeyError:
            port = self.__port__(obj, self.definition, self.__sync__)
            dct[key] = port
            return port


def sync(target='unit', source='any'):
    """
    Syncronizes packets going to an inport by different links.

    Both the syncronization behavior of target and source can be set:
    - 'port': packets are only syncronized within a give port
    - 'unit': packets for one give unit are syncronized
    - 'space': packets are syncronized with the space
    - 'any': packets are syncronized across all sources

    Parameters
    ----------
    target : 'port', 'unit' or 'space'
        default 'unit'
    source : 'port', 'unit', 'space' or 'any'
        default 'any'

    >>> @sync('port')
    ... @inport
    ... def ins(self, data, tag):
    ...     yield from data >> tag >> self.out
    """
    sin = getattr(Syncs, target)
    sout = getattr(Syncs, source)
    def set_sync(inport):
        inport.__sync__ = (sin, sout)
        return inport
    return set_sync


class Port:
    def __init__(self, unit, definition, sync):
        self.fl = unit.__fl__
        self.ctx = self.fl.ctx
        self.name = definition.__name__
        self.id = Sym(self.name)
        self.pid = Pid(self.fl.id, self.name)
        self._definition = coroutine(definition)
        self.sync = sync

    @local
    def definition(self):
        return self._definition.__get__(self.fl.unit)

    def __str__(self):
        return '@%s:%s.%s' % (self.kind, self.fl, self.name)

    def __repr__(self):
        ind = {'in': '<<', 'out': '>>'}[self.kind]
        return '%s %s[%s]' % (self, ind, ', '.join(map(str, self.links)))


class InPort(Port):
    kind = 'in'

    @local
    def links(self):
        return self.fl.ctx.top.links_to(self.pid)

    def __call__(self, data=None, tag=None, **kws):
        if tag and kws:
            tag = tag.add(**kws)
        elif kws:
            tag = Tag(**kws)
        else:
            tag = Tag()

        packet = data >> tag

        if self.fl.ctx.loop.is_running():
            # call from inside flow loop
            logger.debug('INS:call/inside %s [%r/%s]', packet, self.ctx, self)
            return self.handle(packet)
        elif True:
            logger.info('INS:call/outside %s [%r/%s]', packet, self.ctx, self)
            # local startup from outside of loop
            self.fl.space.local = True
            @coroutine
            def handler():
                logger.debug('INS>setup %s [%r/%s]', packet, self.ctx, self)
                yield from self.ctx.ctrl.setup()
                yield from self.handle(packet)
            logger.debug('INS!run %s [%r/%s]', packet, self.ctx, self)
            self.ctx.loop.run_until_complete(handler())
            logger.debug('INS!done %s [%r/%s]', packet, self.ctx, self)
        else:
            # TODO: check if space already running
            # XXX: perhaps always use trigger unit
            raise NotImplementedError

    @coroutine
    def handle(self, packet):
        logger.debug('INS:handle %s [%r/%s]', packet, self.ctx, self)
        yield from self.definition(*packet)


class OutPort(Port, sugar.OutSugar):
    kind = 'out'

    @local
    def links(self):
        return self.fl.ctx.top.links_from(self.pid)

    @coroutine
    def handle(self, packet):
        if not isinstance(packet, Packet):
            packet = packet >> Tag()
        logger.debug('OUT:handle %s >> %s [%r/%s]', packet, self.links, self.ctx, self)
        yield from asyncio.gather(*[l.handle(packet) for l in self.links], 
                loop=self.ctx.loop)

class inport(Unbound):
    """annotation for inport definitions"""
    __port__ = InPort

class outport(Unbound):
    """annotation for outport definitions"""
    __port__ = OutPort


class Control():
    __connectors__ = {}
    @classmethod
    def __register__(cls, connector):
        logger.info('CTX.register %s for %s', connector, connector.kind)
        cls.__connectors__[connector.kind] = connector()

    def __init__(self, ctx):
        self.ctx = ctx
        self.conns = {} 

    @local
    def channels(self):
        return {}

    def resolver(f):
        channels = {}
        @wraps(f)
        def resolve(self, link):
            try:
                return channels[link.sync]
            except KeyError:
                conn = self.__connectors__[link.options['kind']]
                chan = yield from f(self, conn, link)
                channels[link.sync] = chan
                return chan
        return resolve
            
    @resolver
    def resolve_out(self, conn, link):
        logger.debug('CTX.resolve-out %s by %s [%r]', link, conn, self.ctx)
        chan = yield from conn.mk_out(link)
        return link.register(chan)
        
    @resolver
    def resolve_in(self, conn, link):
        logger.debug('CTX.resolve-in %s by %s [%r]', link, conn, self.ctx)
        chan = yield from conn.mk_in(link)
        return link.target.fl.space.register(chan)

    @local
    def spawner(self):
        from . import spawn
        return spawn.MPSpawner()

    @coroutine
    def setup(self):
        spawner = None
        tasks = []
        for space in self.ctx.top.spaces:
            if space.is_setup:
                continue
            if space.local:
                logger.info('CTX.setup entry %s [%r]', space, self.ctx)
                tasks.append(space.setup())
            else:
                logger.info('CTX.setup spawn %s [%r]', space, self.ctx)
                tasks.append(self.spawner.spawn(space))
            space.is_setup = True

        logger.info('CTX.setup %d spaces [%r]', len(tasks), self.ctx)
        yield from asyncio.gather(*tasks, loop=self.ctx.loop)

class LocalCtrl:
    def __init__(self, ctx):
        self.ctx = ctx

    @coroutine
    def open_link(self, link):
        yield from self.ctx.ctrl.resolve_in(link)


def connector(cls):
    Control.__register__(cls)
    return cls

@connector
class LocalConnector:
    kind = 'union'
    queues = weakref.WeakValueDictionary()

    def get_q(self, link):
        try:
            q = self.queues[link.address]
            return q
        except KeyError:
            q = asyncio.Queue(10)
            self.queues[link.address] = q
            return q

    @coroutine
    def mk_out(self, link):
        logger.debug('LCL.mk-out %s', link)
        return LocalOut(self.get_q(link))

    @coroutine
    def mk_in(self, link):
        logger.debug('LCL.mk-in %s', link)
        return LocalIn(self.get_q(link))

class LocalOut:
    def __init__(self, q):
        self.q = q

    @coroutine
    def push(self, pid, packet):
        logger.debug('LCO:push %s >> %s', packet, pid)
        yield from self.q.put((pid, packet)) 

class LocalIn:
    def __init__(self, q):
        self.q = q

    @coroutine
    def pull(self):
        pid,packet = yield from self.q.get()
        logger.debug('LCI:pull %s << %s', pid, packet)
        return pid,packet


@connector
class ZmqConnector:
    kind = 'dist'
    @coroutine
    def mk_out(self, link):
        out = ZmqOut()
        yield from out.setup('ipc://{}/chan'.format('/'.join(link.address)))
        return out

    @coroutine
    def mk_in(self, link):
        ins = ZmqIn()
        yield from ins.setup('ipc://{}/chan'.format('/'.join(link.address)))
        return ins

class ZmqOut:
    @coroutine
    def setup(self, address):
        self.stream = yield from zmqtools.create_zmq_stream(aiozmq.zmq.DEALER, connect=address)

    @coroutine
    def push(self, pid, packet):
        yield from self.stream.push((pid, packet))
    
class ZmqIn:
    @coroutine
    def setup(self, address):
        self.stream = yield from zmqtools.create_zmq_stream(aiozmq.zmq.DEALER, bind=address)

    @coroutine
    def pull(self):
        return (yield from self.stream.pull())



