from collections import namedtuple

import asyncio
from asyncio import coroutine

from pyadds.annotate import cached, delayed, Conotate
from pyadds.str import name_of

from .context import get_current_context, context
from .idd import IdPath
from .packet import Tag

class Unit:
    """
    flo unit with the functionality the flow is build of
    """
    def __init__(self, *, ctx=None, name=None):
        if ctx is None:
            ctx = get_current_context()

        self.name = name or name_of(self)
        self.ctx = ctx
        self.ctx.register(self)

    @cached
    def tp(self):
        return self.ctx.tp.lookup(self.id)

    @coroutine
    def __setup__(self):
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


class Sync(namedtuple('Sync', 'target, source')):
    def __call__(self, source, target):
        out = self.source(source.path).prefixed('out-')
        ins = self.target(target.path).prefixed('in-')
        return out+ins

class Syncs:
    """ port syncronization types"""
    def syncer(*args):
        def sync(path):
            ids = tuple(path[s] for s in args if path[s])
            return IdPath(args, ids)
        sync.__name__ = args[-1] if args else 'any'
        return sync

    any = syncer('topology')
    space = syncer('topology', 'space')
    unit = syncer('topology', 'space', 'unit')
    port = syncer('topology', 'space', 'unit', 'name')


class Port:
    def __init__(self, unit, name, definition, **hints):
        self.unit = unit
        self.name = name
        self.definition = definition
        self.ctx = unit.ctx
        self.hints = hints

    @cached
    def tp(self):
        return self.ctx.tp.get_port(self)

    @cached
    def method(self):
        return self.definition.__get__(self.unit)

    @coroutine
    def run(self, load, tag=None, **kws):
        if tag is None:
            tag = Tag()
        tag = tag.add(**kws)

        packet = load >> tag

        print('running {!r} >> {!r}'.format(packet, self))

        yield from self.ctx.ctrl.activate(self.unit)
        yield from self.handle(*packet)

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
            return loop.run_until_complete(self.run(*args, **kws))

    def async(self, *args, **kws):
        return asyncio.async(self.run(*args, **kws))

    def __str__(self):
        return str(self.tp)

    def __repr__(self):
        return repr(self.tp)


class InPort(Port):
    __kind__ = 'target'

    @cached
    def handle(self):
        return self.method
            

class OutPort(Port):
    __kind__ = 'source'

    @cached
    def channels(self):
        return []

    @cached
    def handle(self):
        return self.ctx.ctrl.local.handler(self.tp.pid)


class unbound(Conotate, cached):
    __port__ = None
    hints = {'sync': Sync(Syncs.unit, Syncs.any)}

    def __default__(self, unit):
        return self.__port__(unit, self.name, self.definition, **self.hints)

class inport(unbound):
    __port__ = InPort

class outport(unbound):
    __port__ = OutPort


def sync(target='unit', source='any'):
    def annotate(port):
        sy = lambda x: getattr(Sync, x)
        port.hints['sync'] = Sync(sy(traget), sy(source))
        return port
    return annotate

async = sync('port')
