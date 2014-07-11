"""
Toplology of a Flow
-------------------

Vertically there's a clear one to n hierachy, starting with the flow context,
having multiple process spaces, each consisting of Units that may have multiple
in- and outports.

Horizontally we have the actual flow network with arbitrary wiring of ports,
build by a link between an inport and an outport.

```
 Context
  | 1
  |
  | n
 Space
  | 1
  |
  | n
 Unit
  | 1
  |
  | n
 Port  :::::> Link :::::> Port
       1    n      n    1
```

The Topology object has methods to setup and query this structure.
"""
from .util import IddPath, Path
from .annotate import local, shared
from .port import inport, outport, OutPort, InPort
from .exec import Executor
from . import context, sugar

from collections import defaultdict, namedtuple

import asyncio
import aiozmq

import logging
logger = logging.getLogger(__name__)

class Context(IddPath):
    __ref__ = 'ctx'
    __master__ = True

    def __init__(self, name=None, setup=None):
        super().__init__(name=name)
        self.setup = setup
        setup()

    @local
    def loop(self):
        asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
        return asyncio.get_event_loop()

    @shared
    def top(self):
        return Topology(self)

    @shared
    def exec(self):
        return Executor(self)

    @asyncio.coroutine
    def aquire(self, num=1):
        yield from self.exec.track.aquire(num)

    @asyncio.coroutine
    def release(self, num=1):
        yield from self.exec.track.release(num)

    def __getattr__(self, name):
        return getattr(self.top, name)


class Flows(IddPath):
    """ flow object base """
    __ref__ = 'unit'
    __by__ = ['space', ...]

    def __init__(self, ins, outs, ctx=None, name=None, ref=None):
        # XXX improve and abstract name inference
        name = name or type(ref).__name__
        super().__init__(name=name)

        if ctx is None:
            ctx = context.get_current_context()

        self.ctx = ctx
        self.ins = ins
        self.outs = outs
        self.ref = ref
        ctx.register(self)

    @local
    def inports(self):
        return [getattr(self.ref, p) for p in self.ins]

    @local
    def outports(self):
        return [getattr(self.ref, p) for p in self.outs]

    @local
    def space(self):
        return self.ctx.get_space(self)

class UnitRef(Flows):
    """ unit based on flow object with @inport and @outport definitions  """
    def __init__(self, flobj, **kws):
        ins = [p.name for p in inport.iter(flobj)]
        outs = [p.name for p in outport.iter(flobj)]
        super().__init__(ins=ins, outs=outs, ref=flobj, **kws)

    @asyncio.coroutine
    def setup(self):
        yield from self.ref.__setup__()

    @asyncio.coroutine
    def teardown(self):
        yield from self.ref.__teardown__()

class Unit(sugar.UnitSugar):
    """ definition of a flow unit """
    def __init__(self, *args, name=None, ctx=None, **kws):
        super().__init__(*args, **kws)

        self.__fl__ = UnitRef(self, name=name, ctx=ctx)
        self.__ctx__ = self.__fl__.ctx

    @asyncio.coroutine
    def __setup__(self):
        pass

    @asyncio.coroutine
    def __teardown__(self):
        pass


class Space(IddPath):
    __ref__ = 'space'
    __by__ = ['ctx', ...]

    def __init__(self, units, ctx, name=None):
        super().__init__(name=name)
        self.ctx = ctx
        self.units = units
        self.replicate = 0

    @shared
    def is_setup(self):
        return False

    @local
    def is_local(self):
        return False

    @local
    def inports(self):
        return {p for unit in self.units for p in unit.inports}

    @local
    def outports(self):
        return {p for unit in self.units for p in unit.outports}

    @local
    def incomming(self):
        return {l for ins in self.inports for l in ins.links}

    @local
    def outgoing(self):
        return {l for ins in self.outports for l in ins.links}

    def __str__(self):
        base = super().__str__() 
        if self.replicate:
            base += '**{}'.format(self.replicate)
        return '{}{{{}}}'.format(base, '&'.join(map(str, self.units)))

    def __repr__(self):
        base = super().__repr__() 
        if self.replicate:
            base += '**{}'.format(self.replicate)
        return '{}{{{}}}'.format(base, '&'.join(map(repr, self.units)))


class Link:
    def __init__(self, ctx, source, target, options):
        self.ctx = ctx
        self.source = source
        self.target = target
        self.options = options

    @local
    def sync(self):
        sync = self.target.sync
        return Path(*(self.ctx.path.ids + sync.target(self.target.path) 
                    + sync.source(self.source.path)))

    def __str__(self):
        return '{}>>{}'.format(self.source, self.target)

    def __repr__(self):
        return '{}>>{}//{}'.format(repr(self.source), repr(self.target), self.options)
    

class Topology:
    def __init__(self, ctx):
        self.ctx = ctx
        self.units = {}
        self.replicates = {}
        self.unions = []
        self.dists = []
        self.local = set()

        self.outs = defaultdict(list)
        self.ins = defaultdict(list)
    
    def register(self, unit):
        self.units[unit.id] = unit

    def put_local(self, unit):
        self.local.add(unit)

    def lookup(self, uid):
        return self.units[uid]

    def _check_restrictions(self, unions, dists):
        for ds in dists:
            for us in unions:
                du = ds.intersection(us)
                if len(du) > 1:
                    raise ValueError(
                        "Following units are requested to "
                        "be unified and distributed:\n%s" 
                        % du)

        for u1 in unions:
            for u2 in unions:
                if u1 == u2:
                    continue
                us = u1.intersection(u2)
                if us:
                    raise ValueError(
                        "Following units are requested to "
                        "be in two unions:\n%s"
                        % us)
        return self

    def unify(self, *units):
        units = set(units)
        logger.info('TOP:unify %s [%r]', units, self.ctx)
        unions = []
        for us in self.unions:
            if us.intersection(units):
                units.update(us)
            else:
                unions.append(us)
        unions.append(units)
        logger.debug('TOP.unions %s [%r]', unions, self.ctx)

        self._check_restrictions(unions, self.dists)
        self.unions = unions

    def distribute(self, *units):
        units = set(units)
        logger.info('TOP:distribute %s [%r]', units, self.ctx)
        dists = []
        for ds in self.dists:
            inter = ds.intersection(units) 
            if inter == ds:
                ds.update(units)
            else:
                dists.append(ds)
        dists.append(units)
        logger.debug('TOP.dists %s [%r]', dists, self.ctx)

        self._check_restrictions(self.unions, dists)
        self.dists = dists

    def replicate(self, units, num):
        logger.info('TOP.replicate %s %d [%r]', units, num, self.ctx)
        # TODO check replicates
        self.replicates.update({unit.id: num for unit in units})


    def complete(self, units):
        """
        completes the set of unified units for a space
         
        if a unit is not restricted to be inside this space and has incomming
        connections from this space, just put it here.
        """
        done = set()
        others = [union for union in self.unions if union != units]
        while done != units:
            for unit in units.difference(done):
                done.add(unit)
                for out in unit.outports:
                  for lnk in self.outs[out.id]:
                    tgt = lnk.target.unit
                    if tgt in units:
                        continue
                    logger.debug('TOP?complete %s->%s:%s', unit, tgt, units)
                    try:
                        self._check_restrictions(others + [units.union({tgt})], self.dists)
                    except ValueError:
                        continue
                    logger.info('TOP+complete %s->%s:%s', unit, tgt, units)
                    units.update({tgt})
        return units

    @shared
    def spaces(self):
        spaces = set()
        rest = set(self.units.values())

        def mk_space(units):
            logger.debug('TOP:mk-space %s [%r]', units, self.ctx)
            space = Space(units, self.ctx)
            spaces.add(space)
            rest.difference_update(units)
            for unit in units:
                repl = self.replicates.get(unit.id, 0)
                if repl:
                    space.replicate = repl
            return space

        for us in self.unions:
            us = self.complete(us)
            mk_space(us)

        # XXX prefare more local spaces ...
        for ds in self.dists:
            for unit in ds:
                if unit in rest:
                    mk_space({unit})
            
        if rest:
            mk_space(set(rest))

        for space in spaces:
            logger.info('TOP.space %r', space)

        return spaces

    def get_space(self, unit):
        for space in self.spaces:
            if unit in space.units:
                return space

        raise ValueError("no space for %s" % (unit,))

    def infer_kind(self, link):
        if 'kind' in link.options:
            return link.options['kind']

        pair = {link.source.unit, link.target.unit}
        srcs = link.source.unit.space.units
        if pair.intersection(srcs) == pair:
            return 'local'

        if link.target.unit.space.replicate:
            return 'replicate'

        return 'distribute'


    def link(self, source, target, **opts):
        if not (isinstance(source, OutPort) and isinstance(target, InPort)):
            raise NotImplementedError("You can only link outports to inports")
        logger.info('TOP.link %s >> %s // %s [%r]', source, target, opts, self.ctx)
        link = Link(self.ctx, source, target, opts)
        self.outs[source.id].append(link)
        self.ins[target.id].append(link)
        return target.unit.ref

    def get_port(self, pid):
        uid, name = pid.id
        unit = self.lookup(uid)
        return getattr(unit.ref, name)

    def links_from(self, src):
        links = []
        for link in self.outs[src]:
            link.options['kind'] = self.infer_kind(link)
            links.append(link)
            logger.info('TOP:links-from %r [%r]', link, self.ctx)
        return links

    def links_to(self, tgt):
        logger.debug('TOP.links-to %r [%r]', tgt, self.ctx)
        links = []
        for link in self.ins[tgt]:
            link.options['kind'] = self.infer_kind(link)
            links.append(link)
            logger.info('TOP:links-to %r [%r]', link, self.ctx)
        return links

