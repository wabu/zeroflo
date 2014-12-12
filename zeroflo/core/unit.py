from collections import namedtuple

import asyncio
from asyncio import coroutine

from pyadds.annotate import cached, delayed, Conotate
from pyadds.str import name_of
from pyadds.logging import log

from .ctx import withtp, withctrl, withctx
from .idd import IdPath
from .packet import Tag

class Unit:
    """
    flo unit with the functionality the flow is build of
    """
    @withctx
    def __init__(self, *, ctx=None, name=None):
        self.name = name or name_of(self)
        self.id = ctx.register(self).id

    @coroutine
    def __setup__(self):
        pass

    @coroutine
    def __teardown__(self):
        pass

    @cached
    def ports(self):
        """ports of the unit"""
        return list(unbound.iter(self, bind=True))

    @property
    def inports(self):
        """ports for incoming data"""
        return [p for p in self.ports if p.kind == 'target']

    @property
    def outports(self):
        """outgoing data ports"""
        return [p for p in self.ports if p.kind == 'source']

    @property
    def inlinks(self):
        """incoming links"""
        return list(chain.from_iterable(p.links for p in self.inports))

    @property
    def outlinks(self):
        """outgoing links"""
        return list(chain.from_iterable(p.links for p in self.outports))

    @withtp
    def __or__(self, other, tp):
        # XXX add extraction to toplogoy
        tp.par(tp[self.id].space, tp[other.id].space)
        return other

    @withtp
    def __and__(self, other, tp):
        # XXX add extraction to toplogoy
        tp.join(tp[self.id].space, tp[other.id].space)
        return other

    @withtp
    def __pow__(self, n, tp):
        tp[self.id].space.replicate = n
        return self

    def __rshift__(self, other):
        self.out >> other
        return other

    def __rrshift__(self, other):
        other >> self.process
        return self

    def __sub__(self, other):
        return self.out -- other

    def __rsub__(self, other):
        return other -- self.process

    def __pos__(self):
        return self

    def __neg__(self):
        return self

    def __add__(self, insert):
        class Insert:
            def __rshift__(_, other):
                ( self -- other )
                ( self >> insert >> other )
                return other
        return Insert()

    @withctrl
    def join(self, ctrl):
        asyncio.get_event_loop().run_until_complete(
                ctrl.await(self))

    def __str__(self):
        return str(self.id)

    def __repr__(self):
        return repr(self.id)


class Sync(namedtuple('Sync', 'target, source')):
    def __call__(self, source, target):
        sy = lambda x: getattr(Syncs, x)
        out = sy(self.source)(source.path).prefixed('out-')
        ins = sy(self.target)(target.path).prefixed('in-')
        return out+ins.sub(1,)

class Syncs:
    """ port syncronization types"""
    def syncer(*args):
        path = None
        def sync(path):
            ids = tuple(path[s] for s in args if path[s])
            return IdPath(args, ids)
        sync.__name__ = args[-1] if args else 'any'
        return sync

    any = syncer('topology')
    space = syncer('topology', 'space')
    unit = syncer('topology', 'space', 'unit')
    port = syncer('topology', 'space', 'unit', 'port')


@log(short='prt', sign='<>')
class Port:
    def __init__(self, unit, name, definition, **hints):
        self.unit = unit
        self.name = name
        self.definition = definition
        self.hints = hints

    @cached
    def method(self):
        return self.definition.__get__(self.unit)

    @coroutine
    @withctrl
    def run(self, load=None, tag=None, ctrl=None, **kws):
        tp = ctrl.tp

        if tag is None:
            tag = Tag()
        tag = tag.add(**kws)

        self.__log.info('running {!r} >> {!r}'.format(load >> tag, self))

        # get context bla ...
        call = CallHelper()
        # XXX set call to be local ...
        tp.add_link(call.out, self)

        yield from ctrl.activate(call)

        yield from call.process(load, tag)

        yield from ctrl.await(call)
        yield from ctrl.close(call)

        tp.remove_links(call.out, self)
        tp.unregister(call)


    def async(self, *args, **kws):
        return self.run(*args, **kws)

    def execute(self, *args, **kws):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.run(*args, **kws))

    @withctrl
    def delay(self, *args, ctrl, **kws):
        return ctrl.queue(self.run(*args, **kws))

    @property
    def __self__(self):
        return self.unit

    @property
    def __name__(self):
        return self.name

    def __call__(self, *args, **kws):
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return self.method(*args, **kws)
        else:
            return self.execute(*args, **kws)

    def __str__(self):
        return '{}.{}'.format(self.unit, self.name)

    def __repr__(self):
        return '{!r}.{}'.format(self.unit, self.name)

    @withtp
    def __rshift__(self, other, tp):
        if not isinstance(other, Port):
            return NotImplemented
        tp.add_link(self, other)
        return other

    @withtp
    def __sub__(self, other, tp):
        if not isinstance(other, Port):
            return NotImplemented
        return tp.remove_links(self, other)

    def __pos__(self):
        return self

    def __neg__(self):
        return self


@log(short='ins', sign='<<')
class InPort(Port):
    __kind__ = 'target'

    @cached
    def handle(self):
        return self.method


@log(short='out', sign='>>')
class OutPort(Port):
    __kind__ = 'source'

    @cached
    def channels(self):
        return []

    @coroutine
    def handle(self, pid, packet):
        self.__log.debug('no handler on %s for  %s by %s', self, packet, pid)

    @withctrl
    def __iter__(self, ctrl):
        tp = ctrl.tp
        run = asyncio.get_event_loop().run_until_complete

        self.__log.info('yielding from {!r}'.format(self))

        yld = YieldHelper()
        tp.add_link(self, yld.process)

        run(ctrl.activate(yld))
        run(ctrl.replay())
        yield from yld
        run(ctrl.close(yld))

        tp.remove_links(self, yld.process)
        tp.unregister(yld)


class unbound(Conotate, cached):
    __port__ = None
    hints = {'sync': Sync('unit', 'any')}

    def __default__(self, unit):
        return self.__port__(unit, self.name, self.definition, **self.hints)

class inport(unbound):
    __port__ = InPort

class outport(unbound):
    __port__ = OutPort


def sync(target='unit', source='any'):
    def annotate(port):
        port.hints['sync'] = Sync(target, source)
        return port
    return annotate

async = sync('port')


class Parts:
    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)
        process, out = self.__flow__()
        if process:
            self.process = process
        if out:
            self.out = out

    def __rshift__(self, other):
        self.out >> other
        return other

    def __rrshift__(self, other):
        other >> self.process
        return self

    def iter(self, *args, **kws):
        self.process.delay(*args, **kws)
        yield from self.out


class part(cached):
    pass


class CallHelper(Unit):
    @outport
    def out(): pass

    @inport
    def process(self, load, tag):
        yield from load >> tag >> self.out


@log(short='yld', sign='>|')
class YieldHelper(Unit):
    @coroutine
    def __setup__(self):
        self.q = asyncio.Queue(8)

    @inport
    def process(self, load, tag):
        pk = load >> tag
        self.__log.debug('yielder got %s', pk)
        yield from self.q.put(pk)

    @withctrl
    def __iter__(self, ctrl):
        run = asyncio.get_event_loop().run_until_complete
        await = asyncio.async(ctrl.await(self))
        while True:
            get = asyncio.async(self.q.get())
            run(asyncio.wait([await, get],
                             return_when=asyncio.FIRST_COMPLETED))
            if get.done():
                yield get.result()
            if await.done():
                break

        while self.q.qsize():
            yield run(self.q.get())

