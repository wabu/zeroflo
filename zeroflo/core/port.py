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
    sync = Sync(getattr(Syncs, target),
                getattr(Syncs, source))
    def set_sync(inport):
        inport.__sync__ = sync
        return inport
    return set_sync

async = sync(target='port')


class Port(IddPath):
    """ port base """
    __by__ = ['unit', 'name']

    def __init__(self, definition, ctx, unit, sync=None, **kws):
        super().__init__(**kws)
        self.ctx = ctx
        self.exec = ctx.exec if ctx else None
        self.sync = sync or Sync(default, any)
        self.definition = definition
        self.unit = unit

        if ctx is None:
            logger.warning('PRT:init %s has no context', self)

    def __call__(self, data=None, tag=None, **kws):
        if tag and kws:
            tag = tag.add(**kws)
        elif kws:
            tag = Tag(**kws)
        else:
            tag = Tag()

        packet = data >> tag

        if self.ctx.loop.is_running():
            # call from inside loop
            logger.debug('PRT:call/inside %s [%r/%s]', packet, self.ctx, self)
            return self.handle(packet)
        elif (not self.unit.space.is_setup) or self.unit.space.local:
            # call from outside, but can be localized
            logger.info('PRT:call/outside %s [%r/%s]', packet, self.ctx, self)
            self.unit.space.local = True
            @coroutine
            def caller():
                logger.debug('PRT>call/setup %s [%r/%s]', packet, self.ctx, self)
                yield from self.ctx.exec.setup()
                logger.debug('PRT>call/handle %s [%r/%s]', packet, self.ctx, self)
                yield from self.handle(packet)
                logger.debug('PRT>call/flush %s [%r/%s]', packet, self.ctx, self)
                yield from self.ctx.exec.flush()
            logger.debug('PRT!run %s [%r/%s]', packet, self.ctx, self)
            self.ctx.loop.run_until_complete(caller())
            logger.debug('PRT!done %s [%r/%s]', packet, self.ctx, self)
        else:
            raise NotImplementedError("currently can't call remotes space, "
                                      "use trigger units instead")

class InPort(Port):
    @local
    def links(self):
        return self.ctx.links_to(self.id)

    @coroutine
    def handle(self, packet):
        logger.debug('IPT.handle %s', packet)
        yield from self.definition(*packet)


class OutPort(Port, sugar.OutSugar):
    @local
    def links(self):
        return self.ctx.links_from(self.id)

    @coroutine
    def handle(self, packet):
        exec = self.exec
        if exec:
            yield from exec.handle(self.id, packet)


class UnboundPort(Conotate, local):
    __port__ = Port
    __sync__ = Sync(Syncs.port, Syncs.any)
    def __default__(self, obj):
        ctx = context.get_object_context(obj)
        unit = ctx.top.get_unit(obj)

        return self.__port__(definition=self.definition.__get__(obj),
                             ctx=ctx,
                             sync=self.__sync__, 
                             unit=unit, 
                             name=self.name,
                             id_=Derived(unit.id, self.name))

class inport(UnboundPort):
    __port__ = InPort

class outport(UnboundPort):
    __port__ = OutPort
