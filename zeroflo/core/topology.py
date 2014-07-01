"""
Toplology of a Flow
-------------------

Vertically there's a clear one to n hierachy, starting with the flow context,
having multiple process spaces, each consisting of Units that may have multiple
in- and outports.

Horizontally we have the actual flow network with arbitrary wiring of ports,
build by a link between an inport and an outport.

Furthermore, a unit always reference an object it is based on.

```
 Context
  | 1
  |
  | n
 Space
  | 1
  |
  | n
 Unit  -ref-> object
  | 1  1    1
  |
  | n
 Port  :::::> Link :::::> Port
       1    n      n    1
```

The Topology object has methods to setup and query this structure.
"""
from .util import IddPath, Path
from .annotate import local, shared
from .port import inport, outport
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

    def __getattr__(self, name):
        return getattr(self.top, name)


class Unit(IddPath):
    """ flow unit """
    __ref__ = 'unit'
    __by__ = ['space', ...]

    def __init__(self, ctx, ins, outs, name=None, ref=None):
        # XXX improve and abstract name inference
        name = name or type(ref).__name__
        super().__init__(name=name)
        self.ins = ins
        self.outs = outs
        self.ref = ref
        self.ctx = ctx

    @local
    def inports(self):
        return [getattr(self.ref, p) for p in self.ins]

    @local
    def outports(self):
        return [getattr(self.ref, p) for p in self.outs]

    @local
    def space(self):
        return self.ctx.top.get_space(self.id)


class Flo(sugar.UnitSugar):
    def __init__(self, *args, name=None, **kws):
        super().__init__(*args, **kws)
        self.__ctx__ = context.get_object_context(self)
        self.__ctx__.register(self)

class FloUnit(Unit):
    """ unit based on flow object with @inport and @outport definitions  """
    def __init__(self, flobj, ctx, name=None):
        ins = [p.name for p in inport.iter(flobj)]
        outs = [p.name for p in outport.iter(flobj)]
        super().__init__(ins=ins, outs=outs, ctx=ctx, name=name, ref=flobj)


class Space(IddPath):
    __ref__ = 'space'
    __by__ = ['ctx', ...]

    def __init__(self, uids, ctx, name=None):
        super().__init__(name=name)

        self.ctx = ctx
        self.uids = uids

    @shared
    def is_setup(self):
        return False

    @local
    def units(self):
        return list(map(self.ctx.top.lookup, self.uids))

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

    @local
    def local(self):
        return False



class Link:
    def __init__(self, ctx, src, tgt, options):
        self.ctx = ctx
        self.src = src
        self.tgt = tgt
        self.options = options

    @local
    def source(self):
        return self.ctx.get_port(self.src)

    @local
    def target(self):
        return self.ctx.get_port(self.tgt)

    @local
    def sync(self):
        sync = self.target.sync
        return Path(*(self.ctx.path.ids + sync.target(self.target.path) + sync.source(self.source.path)))

    def __str__(self):
        return '{}>>{}'.format(self.source, self.target)

    def __repr__(self):
        return '{}>>{}'.format(repr(self.source), repr(self.target))
    

class Topology:
    def __init__(self, ctx):
        self.ctx = ctx
        self.units = {}
        self.unions = []
        self.dists = []

        self.outs = defaultdict(list)
        self.ins = defaultdict(list)
    
    def register(self, obj):
        if isinstance(obj, Unit):
            unit = obj
        else:
            unit = self.get_unit(obj)
        logger.info('TOP:register %s [%r]', unit, self.ctx)
        self.units[unit.id] = unit

    def lookup(self, uid):
        return self.units[uid]

    @local
    def flobjs(self):
        return {}

    def get_unit(self, obj):
        try:
            return self.flobjs[obj]
        except KeyError:
            for unit in self.units.values():
                if unit.ref == obj:
                    self.flobjs[obj] = unit
                    return unit

        unit = FloUnit(obj, ctx=self.ctx)
        obj.__unit__ = unit

        self.register(unit)
        return unit

    def _check_restrictions(self, unions, dists):
        for ds in dists:
            for us in unions:
                du = ds.intersection(us)
                if len(du) > 1:
                    raise ValueError(
                        "Following units are requested to "
                        "be unified and distributed:\n%s" 
                        % {self.lookup_fl(uid) for uid in du})
        return self

    def unify(self, *units):
        ids = {unit.id for unit in units}
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

    def distribute(self, *units):
        ids = {unit.id for unit in units}
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
        rest = set(self.units.keys())

        def mk_space(units):
            logger.debug('TOP:mk-space %s [%r]', units, self.ctx)
            space = Space(units, self.ctx)
            spaces.add(space)
            rest.difference_update(units)

        for us in self.unions:
            mk_space(us)

        # XXX prefare more local spaces ...
        for ds in self.dists:
            for uid in ds:
                if uid in rest:
                    mk_space({uid})
            
        if rest:
            mk_space(set(rest))

        logger.info('TOP.spaces %s [%r]', '|'.join(map(str, spaces)), self.ctx)

        return spaces

    def get_space(self, uid):
        for space in self.spaces:
            if uid in space.uids:
                return space

        raise ValueError("no space for %s" % (uid,))

    def infer_kind(self, link):
        if 'kind' in link.options:
            return link.options['kind']
        pair = {link.source, link.target}

        srcs = link.source.unit.space.uids
        if pair.intersection(srcs) == pair:
            return 'union'

        tgts = link.target.unit.space.uids
        for ds in self.dists:
            sd = ds.intersection(srcs)
            td = ds.intersection(tgts)
            if sd and td:
                return 'dist'

        return 'union'

    def link(self, source, target, **opts):
        logger.info('TOP.link %s >> %s // %s [%r]', source, target, opts, self.ctx)

        self.outs[source.id].append((target.id, opts))
        self.ins[target.id].append((source.id, opts))

    def get_port(self, pid):
        uid, name = pid.id
        unit = self.lookup(uid)
        return getattr(unit.ref, name)

    def links_from(self, src):
        links = []
        for tgt,opts in self.outs[src]:
            link = Link(self.ctx, src, tgt, opts)
            link.options['kind'] = self.infer_kind(link)
            links.append(link)
            logger.debug('TOP:links-from %r [%r]', link, self.ctx)
        return links

    def links_to(self, tgt):
        links = []
        for src,opts in self.ins[tgt]:
            link = Link(self.ctx, src, tgt, opts)
            link.options['kind'] = self.infer_kind(link)
            links.append(link)
            logger.debug('TOP:links-to %r [%r]', link, self.ctx)
        return links

