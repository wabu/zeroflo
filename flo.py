import asyncio
from asyncio import coroutine

from . import sugar
from .tools import *

from collections import namedtuple
from contextlib import contextmanager
import weakref

import logging
logger = logging.getLogger(__name__)

_policy = None

def _init_flow_policy():
    raise NotImplementedError

def set_flow_policy(policy):
    global _policy
    _policy = policy

def get_flow_policy():
    if _policy is None:
        set_flow_policy(_init_flow_policy())
    return _policy

_ctx_cache = weakref.WeakValueDictionary()
def _register_ctx(ctx):
    _ctx_cache[ctx.nid] = ctx

def _cached_ctx(ctx):
    if isinstance(ctx, str):
        nid = ctx
    else:
        nid = ctx.nid
    return _ctx_cache[nid]

class FlowPolicy:
    def __init__(self):
        self._ctx = None

    def new_flow_context(self, name=None):
        raise NotImplementedError

    def get_flow_context(self, name=None):
        if self._ctx == None:
            self._ctx = self.new_flow_context(name)
            _register_ctx(self._ctx)
        return self._ctx

    def set_flow_context(self, ctx):
        self._ctx = ctx

def get_flow_context(name=None):
    return get_flow_policy().get_flow_context(name=None)

def new_flow_context(name=None):
    return get_flow_policy().new_flow_context(name=None)

def set_flow_context(ctx):
    return get_flow_policy().set_flow_context(ctx)

@contextmanager
def context(ctx=None):
    policy = get_flow_policy()
    old = policy._ctx
    try:
        if ctx is None or isinstance(ctx, str):
            yield policy.new_flow_context(name=ctx)
        else:
            yield policy.set_flow_context(ctx) 

    finally:
        policy._ctx = old


class Unit(sugar.UnitSugar, Named):
    """
    single unit of the flow with in- and outports ...
    """
    def __init__(self, name=None):
        super().__init__(name=name)
        self.__fl__ = Fl(self)

class Fl(Named):
    """info about a flow unit"""
    def __init__(self, unit, *, ctx=None):
        super().__init__(name=unit.name)

        if ctx is None:
            ctx = get_flow_context()
        ctx.register(self)

        self.ctx = ctx
        self.unit = unit
        self.space = None

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
        return [p for p in self.ports if isinstance(p, inport)]

    @property
    def outports(self):
        return [p for p in self.ports if isinstance(p, outport)]

    def _links(self, ports):
        links = []
        for p in ports:
            links.extend(p.links)
        return links

    @property
    def links(self):
        return self._links(self.ports)

    @property
    def incomming(self):
        return self._links(self.ports)

    @property
    def outgoing(self):
        return self._links(self.ports)

    @property
    def inunits(self):
        return [c.target.fl.unit for c in self.incomming]

    @property
    def outunits(self):
        return [c.source.fl.unit for c in self.outgoing]

    def __str__(self):
        return str(self.unit)

    def __repr__(self):
        return repr(self.unit)


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


class Context(Named):
    """
    flow setup and management object

    Note: You normally don't use this class directly but interact with
    sugery operators on the unit and port objects:

    >>> with flo.context('example'):
    ...    a,b,c = AUnit(), BUnit(), CUnit
    ...    a | (b & c)
    ...    a.out >> b.ins
    ...    a.cmd >> b.ctrl
    ...    b.out >> c.ins
    >>> a.process("data.csv", outfile=None)
    """
    def __init__(self, name=None):
        super().__init__(name or 'flow')
        self.fls = {}

        self.unions = []
        self.dists = []

    def __setstate__(self, state):
        try:
            self.__dict__ =  _cached_ctx(state['nid']).__getstate__()
        except KeyError:
            self.__dict__ = state

    def register(self, fl):
        self.fls[fl.nid] = fl

    def _check_restrictions(self, unions, dists):
        for ds in dists:
            for us in unions:
                du = ds.intersection(us)
                if len(du) > 1:
                    raise ValueError("The fls %s are requested to be unified and distributed!")
        return self

    def unify(self, *fls):
        logger.info('%s unifies %s', self, set(fls))
        ns = nids(fls)
        unions = []
        for us in self.unions:
            if us.intersection(ns):
                ns.update(us)
            else:
                unions.append(us)
        unions.append(ns)
        logger.debug('%s.unions %s', self, unions)

        self._check_restrictions(unions, self.dists)
        self.unions = unions

    def distribute(self, *fls):
        logger.info('%s distributes %s', self, set(fls))
        ns = nids(fls)
        dists = []
        for ds in self.dists:
            inter = ds.intersection(ns) 
            if inter == ds:
                ds.update(ns)
            else:
                dists.append(ds)
        dists.append(ns)
        logger.debug('%s.dists %s', self, dists)

        self._check_restrictions(self.unions, dists)
        self.dists = dists

    def space_set(self, fl):
        for fls in self.unions:
            if fl in fls:
                return fls
        else:
            return {fl}

    def any_space(self, fls):
        for fl in fls:
            if fl.space:
                return space
        else:
            return None

    @coroutine
    def space_of(self, fl):
        fls = self.space_set(fl)
        space = self.any_space(fls)

        if space is None:
            space = yield from self.setup_space(fls)

        for fl in fls:
            fl.space = space
        return space

    @coroutine
    def setup_space(self, fls):
        raise NotImplementedError

    def link(self, source, target):
        pair = {source.fl.nid, target.fl.nid}
        logger.debug('%s linking pair %s', self, pair)
        logger.debug('%s.unions %s', self, self.unions)
        logger.debug('%s.dists %s', self, self.dists)

        if any(pair.intersection(us) == pair for us in self.unions):
            cause = 'union-intersect'
            kind = 'local'
        elif any(pair.intersection(ds) == pair for ds in self.dists):
            cause = 'dist-intersect'
            kind = 'remote'
        elif any(target in ds for ds in self.dists):
            cause = 'dist-single'
            kind = 'remote'
        else:
            cause = 'fallback'
            kind = 'local'

        logger.info('linking %s >> %s // %s -> %s', source, target, cause, kind)
        link = self.mk_link(kind, source, target)
        
        source.add_link(link)
        target.add_link(link)

    def mk_link(self, kind, source, target):
        if kind == 'local':
            return Link(self, source, target)
        else:
            raise NotImplementedError("{} link not implemented!".format(kind))


class Space(Named):
    def __init__(self, fls, ctx=None, name=None):
        name = name or '['+','.join([fl.name for fl in fls])+']'
        super().__init__(name=name)

        if ctx is None:
            ctx = fls[0].ctx
        self.ctx = ctx
        self.fls = nids(fls)
        self._sync()

    def __getstate__(self):
        state = self.__dict__.copy()
        state['fls'] = nids(self.fls.values())
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._sync()

    def _sync(self):
        logger.debug('%s syncing %s', self, set(self.fls))
        self.fls = {nid: self.ctx.fls[nid] for nid in self.fls}
        for fl in self.fls.values():
            fl.space = self
        logger.debug('%s synced %s', self, set(self.fls))

    @coroutine
    def register(self, fl):
        self.fls[fl.nid] = fl
        logger.info('%s registered %s: %s', self, fl.nid or -1, fl)

    @coroutine
    def handle(self, nid, port, packet):
        fl = self.fls[nid]
        logger.debug('%s handles %s >> %s.%s', self, packet, fl, port)
        yield from getattr(self.fls[nid].unit, port).handle(packet)


class Unbound:
    def __new__(cls, definition):
        return wraps(definition)(super().__new__(cls))

    def __init__(self, definition):
        self.definition = definition
        self.name = definition.__name__
        self.entry = '_'+self.name

    """annotation descriptor for ports"""
    def __get__(self, obj, objtype):
        if not obj:
            return self.definition
        try:
            return obj.__dict__[self.entry]
        except KeyError:
            port = self.__port__(obj, self.definition)
            obj.__dict__[self.entry] = port
            return port

class Port:
    """port of an flow object"""
    def __init__(self, unit, definition):
        self.unit = unit
        self.name = definition.__name__
        # bound method
        self.definition = coroutine(definition).__get__(unit)
        self.links = []

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['definition']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.definition = coroutine(getattr(type(self.unit), self.name)).__get__(self.unit, type(self.unit))

    @property
    def fl(self):
        return self.unit.__fl__

    @property
    def ctx(self):
        return self.fl.ctx

    @property
    def space(self):
        return self.fl.space

    def add_link(self, link):
        self.links.append(link)

    def __str__(self):
        return '@%s:%s.%s' % (self.kind, self.fl, self.name)

    def __repr__(self):
        ind = {'in': '<<', 'out': '>>'}[self.kind]
        return '%s %s[%s]' % (self, ind, ', '.join(map(str, self.links)))


class InPort(Port):
    kind = 'in'
    def __init__(self, unit, definition):
        super().__init__(unit, coroutine(definition))

    def __call__(self, data=None, tag=None, **kws):
        if tag and kws:
            tag = tag.add(**kws)
        else:
            tag = Tag(**kws)

        logger.debug('%s calls %s', self.fl.space or '[]', self)

        if self.fl.space and self.fl.space.inside:
            # call from inside the unit
            return self.handle(data >> tag)
            #result = yield from self.handle(data >> tag)
            #return result

        # trigger call
        if not self.fl.space:
            asyncio.get_event_loop().run_until_complete(
                    self.handle(data >> tag))
        else:
            # TODO: send to space
            raise NotImplementedError

    @coroutine
    def handle(self, packet):
        logger.debug('%s handels %s', self, packet)
        yield from self.definition(*packet)

class OutPort(Port):
    kind = 'out'

    def __init__(self, unit, definition):
        super().__init__(unit, definition)
   
    def __rshift__(self, port):
        self.ctx.link(self, port)

    @coroutine
    def __rrshift__(self, packet):
        logger.debug('%s got %s', self, packet)
        yield from asyncio.gather(*[c.handle(packet) for c in self.links])

class inport(Unbound):
    """annotation for inport definitions"""
    __port__ = InPort


class outport(Unbound):
    """annotation for outport definitions"""
    __port__ = OutPort


class Link:
    def __init__(self, ctx, source, target):
        self.ctx = ctx
        self.source = source
        self.target = target

    @coroutine
    def handle(self, packet):
        logger.debug('%s handles %s', self, packet)
        yield from self.target.handle(packet)

    def __str__(self):
        return '{} >> {}'.format(self.source, self.target)


