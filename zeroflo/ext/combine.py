"""
The combine defines simple ways to combine packets from inports:
>>> class Add:
...     @outport
...     def outs(): pass
...
...     @combine
...     def add(self, a, b, tag):
...         a+b >> tag >> self.outs   
...
...     @inport
...     def a(self, a, tag):
...         yield from self.combine(a=a, tag=tag)
...
...     @inport
...     def b(self, b, tag):
...         yield from self.combine(b=b, tag=tag)

It includes
- `combine`: combine args from different async inports
- `join(on : str)`: join args on same tag attribute
"""

import inspect
import asyncio
from asyncio import coroutine
from functools import wraps
from ..core.packet import Tag
from pyadds.annotate import Annotate, ObjDescr, Get

import logging
logger = logging.getLogger(__name__)

class Combiner:
    def __init__(self, obj, f):
        spec = inspect.getfullargspec(f)

        self.definition = coroutine(f)
        self.name = f.__name__
        self.obj = obj
        self.loop = obj.ctx.loop
        self.slots = {k: asyncio.Event(loop=self.loop) for k in spec.args[1:-1]}
        for slot in self.slots.values():
            slot.set()
        self.vals = {}
        self.tag = Tag()

    @coroutine
    def flush(self):
        vals = self.vals
        slots = self.slots

        logger.debug('CBN:flush %s [%s]', set(vals), self)
        yield from self.definition(self.obj, tag=self.tag, **vals)
        logger.debug('CBN:flushed %s [%s]', set(vals), self)

        vals.clear()
        self.tag.clear()
        for key,slot in slots.items():
            logger.debug('CBN:flush %s unblock [%s]', key, self)
            slot.set()

    @coroutine
    def put(self, key, val, tag):
        vals = self.vals
        slot = self.slots[key]

        logger.debug('CBN:put %s=%s [%s]', key, val>>tag, self)
        while key in vals:
            yield from slot.wait()
            logger.debug('CBN:put waited %s->%s [%s]', key, set(vals), self)
        logger.debug('CBN:put blocks %s [%s]', key, self)
        slot.clear()
        vals[key] = val
        self.tag.update(tag)

        if len(vals) == len(self.slots):
            yield from self.flush()

    def __call__(self, tag=Tag(), **kws):
        logger.debug('CBN:combining %s :: %s [%s]', set(kws), set(self.vals), self)
        puts = [self.put(k, v, tag) for k,v in kws.items()]
        yield from asyncio.gather(*puts, loop=self.loop)
        logger.debug('CBN:combined %s -> %s [%s]', set(kws), set(self.vals), self)

    def __str__(self):
        return 'combine:{} {}'.format(self.name, 
                '|'.join('{}{}={}'.format('.' if slot.is_set() else '!', 
                        key, self.vals.get(key, '_')>>Tag())
                    for key,slot in sorted(self.slots.items())))

class combine(Annotate, ObjDescr, Get):
    def __default__(self, obj):
        logger.debug('CBN:combine for %s [%s]', self.name, self.definition)
        return Combiner(obj, self.definition)
