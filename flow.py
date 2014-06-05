import asyncio
from asyncio import coroutine

from collections import defaultdict, namedtuple
import multiprocessing as mp

import inspect
from contextlib import contextmanager

from datalibs.util.antools import AnnotationDescriptorBase, delayed

import logging
logger = logging.getLogger(__name__)

_flow = None

class ConstructionError(ValueError):
    pass

def init_flow_context(name=None):
    raise NotImplementedError

def set_flow_policy(pol):
    global init_flow_context
    init_flow_context = pol

def get_flow_context():
    global _flow
    if _flow is None:
        _flow = init_flow_context()
    return _flow

def new_flow_context(name=None):
    global _flow
    _flow = init_flow_context()
    return _flow

def set_flow_context(flow):
    global _flow
    _flow = flow
    return _flow


@contextmanager
def flow_context(flow=None):
    global _flow
    old = _flow

    try:
        if flow is None or isinstance(flow, str):
            yield new_flow_context(flow)
        else:
            yield set_flow_context(flow) 

    finally:
        _flow = old


class Named:
    __name__ = None
    def __init__(self, name=None):
        self.name = name or self.__name__ or type(self).__name__
        self.nid = '{}@{}'.format(self.name, ('%x' % id(self))[2:-2])

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.nid


class Flo(Named):
    """
    The base class for (fl)ow (o)bject with port definitions.

    Flow objects are triggered via packet send to their inport. 
    As the packet is processd, derived packages are send to outports
    of the flo.

    The processing inside a flo is sychronious, 
    but the overall flow between different flos is highly asyncrone.

    The __fl__ attribute is used to access flow related attributes.
    """
    def __init__(self, name=None):
        super().__init__(name=name)
        self.__fl__ = Fl(self)

    def __and__(self, other):
        """
        unifiy operation putting flos in the same process
        """
        if isinstance(other, Flo):
            return self.__fl__.flow.unify(self.__fl__, other.__fl__)
        else:
            return NotImplemented

    def __or__(self, other):
        """
        distribute operatoion putting flos in different processes
        """
        if isinstance(other, Flo):
            return self.__fl__.flow.distribute(self.__fl__, other.__fl__)
        else:
            return NotImplemented


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
        """
        create a packet with self and data
        """
        return Packet(data, self)

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError
        return self.get(name, None)

    def __repr__(self):
        return ', '.join('{}: {}'.format(k,v) for k,v in self.items())

    def __str__(self):
        return ','.join(map(str, self.keys()))


class Fl(Named):
    """flow info about a fl(ow) o(bject)"""
    def __init__(self, flo, *, flow=None):
        super().__init__(name=flo.name)

        if flow is None:
            flow = get_flow_context()
        self.flow = flow
        self.flo = flo
        self.space = None
        self._fid = None

    @property
    def fid(self):
        return self._fid

    @fid.setter
    def fid(self, value):
        if self._fid is not None and self._fid != value:
            raise ConstructionError(
                    '{} fid mismatch {} {}!'.format(self, self._fid, value))
        self._fid = value

    @delayed
    def ports(self):
        ports = []

        cls = type(self.flo)
        for name in dir(cls):
            if not name.startswith('_'):
                attr = getattr(cls, name)
                if isinstance(attr, Unbound):
                    ports.append(attr.__get__(self.flo, cls))

        return ports

    @property
    def inports(self):
        return [p for p in self.ports if isinstance(p, inport)]

    @property
    def outports(self):
        return [p for p in self.ports if isinstance(p, outport)]

    def _connections(self, ports):
        conns = []
        for p in ports:
            conns.extend(p.conns)
        return conns

    @property
    def connections(self):
        return self._connections(self.ports)

    @property
    def incomming(self):
        return self._connections(self.ports)

    @property
    def outgoing(self):
        return self._connections(self.ports)

    @property
    def ins(self):
        return [c.target.fl.flo for c in self.incomming]

    @property
    def outs(self):
        return [c.source.fl.flo for c in self.outgoing]

    def __str__(self):
        return str(self.flo)

    def __repr__(self):
        return repr(self.flo)


class Flow(Named):
    """
    class for overall flow interactions

    Note: You normally don't use this class directly but interact with
    operators on the flo and port objects
    """
    def __init__(self, name=None):
        super().__init__(name or 'flow')

        self.unions = []
        self.dists = []
        self._latest_setup = []

    def _check_fls(self, unions, dists):
        for ds in dists:
            for us in unions:
                du = ds.intersection(us)
                if len(du) > 1:
                    raise ValueError("The fls %s are requested to be unified and distributed!")
        return self

    def __and__(self, other):
        if isinstance(other, Flo):
            other = other.__fl__
        return self.unify(self._latest_setup[-1], other)

    def __rand__(self, other):
        if isinstance(other, Flo):
            other = other.__fl__
        return self.unify(other, self._latest_setup[0])

    def __or__(self, other):
        if isinstance(other, Flo):
            other = other.__fl__
        return self.distribute(self._latest_setup[-1], other)

    def __ror__(self, other):
        if isinstance(other, Flo):
            other = other.__fl__
        return self.distribute(other, self._latest_setup[0])

    def unify(self, *fls):
        self._latest_setup = fls
        fls = set(fls)
        unions = []
        for us in self.unions:
            if us.intersection(fls):
                fls.update(us)
            else:
                unions.append(us)
        unions.append(fls)

        self._check_fls(unions, self.dists)
        self.unions = unions
        return self

    def distribute(self, *fls):
        self._latest_setup = fls
        fls = set(fls)
        dists = self.dists + [fls]

        self._check_fls(self.unions, dists)
        self.dists = dists
        return self

    @coroutine
    def space_of(self, fl):
        # get fl set for space
        for fls in self.unions:
            if fl in fls:
                break
        else:
            fls = {fl}

        # check if we already have a space
        for fl in fls:
            if fl.space:
                space = fl.space
        else:
            space = yield from self.setup_space(fls)
            space.sync(fls)

        # set space for all objects
        for fl in fls:
            fl.space = space

        return space

    def connect(self, source, target):
        pair = {source.fl, target.fl}

        if any(pair.intersection(us) == pair for us in self.unions):
            kind = 'local'
        elif any(pair.intersection(ds) == pair for ds in self.dists):
            kind = 'remote'
        elif any(target in ds for ds in self.dists):
            kind = 'remote'
        else:
            kind = 'local'

        logger.info('connecting %s >> %s // %s', source, target, kind)
        conn = self.mk_connection(kind, source, target)
        
        source.add(conn)
        target.add(conn)

    def mk_connection(self, kind, source, target):
        if kind == 'local':
            return Connection(self, source, target)
        else:
            raise NotImplementedError("{} connection not implemented!".format(kind))


class Space(Named):
    def __init__(self, flow, fls):
        name = '['+','.join([fl.name for fl in fls])+']'
        super().__init__(name=name)

        self.fls = list(fls)
        for i, fl in enumerate(fls):
            fl.fid = i 
            fl.space = self
        self.flow = flow
        logger.debug('!! space %s', self)

    def sync(self, fls):
        match = {fl.nid: fl.fid for fl in self.fls}
        for fl in fls:
            fl.fid = match[fl.nid]
        self.fls = fls

    @coroutine
    def register(self, fl):
        fl.fid = len(self.fls)
        self.fls.append(fl)
        logger.info('%s registered %d: %s', self, fl.fid or -1, fl)

    @coroutine
    def handle(self, fid, port, packet):
        fl = self.fls[fid]
        logger.debug('%s handles %s >> %s.%s', self, packet, fl, port)
        yield from getattr(self.fls[fid].flo, port).handle(packet)


class Unbound(AnnotationDescriptorBase):
    """annotation descriptor for ports"""
    def __get__(self, obj, objtype):
        if not obj:
            return self.definition
        try:
            return super().__get__(obj, objtype)
        except KeyError:
            port = self.__port__(obj, self.definition)
            return self.__set_default__(obj, port)


class Port:
    """port of an flow object"""
    def __init__(self, flo, definition):
        self.flo = flo
        self.name = definition.__name__
        # bound method
        self.definition = definition.__get__(flo, type(flo))
        self.conns = []

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['definition']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.definition = coroutine(getattr(type(self.flo), self.name)).__get__(self.flo, type(self.flo))

    @delayed
    def fl(self):
        return self.flo.__fl__

    @property
    def flow(self):
        return self.fl.flow

    @property
    def space(self):
        return self.fl.space

    def add(self, conn):
        self.conns.append(conn)

    def __str__(self):
        return '@%s:%s.%s' % (self.kind, self.fl, self.name)

    def __repr__(self):
        ind = {'in': '<<', 'out': '>>'}[self.kind]
        return '%s %s[%s]' % (self, ind, ', '.join(map(str, self.conns)))


class InPort(Port):
    kind = 'in'
    def __init__(self, flo, definition):
        super().__init__(flo, coroutine(definition))

    def __call__(self, data=None, tag=None, **kws):
        if tag and kws:
            tag = tag.add(**kws)
        else:
            tag = Tag(**kws)

        logger.debug('%s calls %s', self.fl.space or '[]', self)

        if self.fl.space and self.fl.space.inside:
            # call from inside the flo
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

    def __init__(self, flo, definition):
        super().__init__(flo, definition)
   
    def __rshift__(self, port):
        self.flow.connect(self, port)

    @coroutine
    def __rrshift__(self, packet):
        logger.debug('%s got %s', self, packet)
        yield from asyncio.gather(*[c.handle(packet) for c in self.conns])

class inport(Unbound):
    """annotation for inport definitions"""
    __port__ = InPort


class outport(Unbound):
    """annotation for inport definitions"""
    __port__ = OutPort


class Connection:
    def __init__(self, flow, source, target):
        self.flow = flow
        self.source = source
        self.target = target

    @coroutine
    def handle(self, packet):
        logger.debug('%s handles %s', self, packet)
        yield from self.target.handle(packet)

    def __str__(self):
        return '{} >> {}'.format(self.source, self.target)


