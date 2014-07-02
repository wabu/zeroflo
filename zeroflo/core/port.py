from . import context
from . import sugar
from .util import IddPath, Derived
from .annotate import Conotate, local
from .exec import Tag

import asyncio
from asyncio import coroutine
from collections import namedtuple

import logging
logger = logging.getLogger(__name__)

class Syncs:
    """ synconization extractors for ports """
    def syncer(*args):
        @classmethod
        def sync(cls, path):
            return tuple(path[s] for s in args if path[s])
        return sync

    any = syncer()
    space = syncer('space')
    unit = syncer('space', 'unit')
    port = syncer('space', 'unit', 'port') 


Sync = namedtuple('Sync', 'target, source')

def sync(target='port', source='any'):
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
    getattr(Syncs, target)
    getattr(Syncs, source)
    sync = Sync(target, source)
    def set_sync(inport):
        inport.__sync__ = sync
        return inport
    return set_sync

async = sync(target='port')


class Port(IddPath):
    """ port base """
    __by__ = ['unit', 'name']

    def __init__(self, ctx, unit, name, sync=None, **kws):
        super().__init__(id_=Derived(unit.id, name), **kws)

        self.ctx = ctx
        self.__sync__ = sync or Sync('default', 'any')
        self.unit = unit
        self.name = name

    @local
    def sync(self):
        tgt, src = self.__sync__
        return Sync(getattr(Syncs, tgt), getattr(Syncs, src))

    @local
    def definition(self):
        obj = self.unit.ref
        return getattr(type(obj), self.name).definition.__get__(obj)

    def trigger(self, data=None, tag=None, **kws):
        self.unit.space.is_local = True

        if tag and kws:
            tag = tag.add(**kws)
        elif kws:
            tag = Tag(**kws)
        else:
            tag = Tag()

        packet = data >> tag

        logger.debug('PRT>call/setup %s [%r/%s]', packet, self.ctx, self)
        yield from self.ctx.exec.setup()
        logger.debug('PRT>call/handle %s [%r/%s]', packet, self.ctx, self)
        yield from self.handle(packet)
        logger.debug('PRT>call/flush %s [%r/%s]', packet, self.ctx, self)
        yield from self.ctx.exec.flush()

    def __call__(self, data=None, tag=None, **kws):
        if self.ctx.loop.is_running():
            # call from inside loop
            logger.debug('PRT:call/inside [%r/%s]', self.ctx, self)
            return self.handle(data, tag)
        elif (not self.unit.space.is_setup) or self.unit.space.is_local:
            # call from outside, but can be localized
            logger.info('PRT:call/outside [%r/%s]', self.ctx, self)
            self.ctx.loop.run_until_complete(self.trigger(data=data, tag=tag, **kws))
            logger.debug('PRT:call/done [%r/%s]', self.ctx, self)
        else:
            raise NotImplementedError("currently can't call remotes space, "
                                      "use trigger units instead")

class InPort(Port):
    @local
    def links(self):
        return self.ctx.links_to(self.id)

    @coroutine
    def handle(self, packet):
        logger.debug('IPT.handle %s [%r/%r]', packet, self.ctx, self)
        yield from self.definition(*packet)


class OutPort(Port, sugar.OutSugar):
    @local
    def links(self):
        return self.ctx.links_from(self.id)

    @coroutine
    def handle(self, packet):
        yield from self.ctx.exec.handle(self.id, packet)


class UnboundPort(Conotate, local):
    __port__ = Port
    __sync__ = Sync('port', 'any')
    def __default__(self, obj):
        return self.__port__(ctx=obj.__ctx__,
                             unit=obj.__unit__, 
                             name=self.name,
                             sync=self.__sync__)

class inport(UnboundPort):
    __port__ = InPort

class outport(UnboundPort):
    __port__ = OutPort
