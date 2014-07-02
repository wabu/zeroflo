"""
join with the main thread
"""
from .trigger import Trigger
from ..core import *

from functools import wraps

class Join(Trigger):
    @delayed
    def q(self):
        return asyncio.Queue()

    @inport
    def ins(self, load, tag):
        yield from self.q.put(load >> tag)


    def it(self, *args, **kws):
        port = super().once
        trigger = asyncio.async(port.trigger(*args, **kws), loop=self.__ctx__.loop)
        while True:
            fut = asyncio.async(self.q.get())
            asyncio.get_event_loop().run_until_complete(fut)
            yield fut.result()
