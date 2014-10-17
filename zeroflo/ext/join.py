"""
join with the main thread
"""
from ..core import *

from functools import wraps
from pyadds.annotate import delayed

class Join(Unit):
    @delayed
    def q(self):
        return asyncio.Queue(8)

    @inport
    def process(self, load, tag):
        yield from self.q.put(load >> tag)

    def __iter__(self):
        flushed = asyncio.async(self.ins.ctx.exec.flush())
        while True:
            result = asyncio.async(self.q.get())
            wait = asyncio.wait([result, flushed], return_when=asyncio.FIRST_COMPLETED)

            asyncio.get_event_loop().run_until_complete(wait)
            if result.done():
                yield result.result()
            if flushed.done() and self.q.empty():
                break
