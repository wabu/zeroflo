from ..core import *
from ..ext import param, Paramed

import time
import logging

from collections import defaultdict

logger = logging.getLogger(__name__)

class match(Unit):
    def __init__(self, **matches):
        super().__init__()
        self.matches = {k: v if isinstance(v, set) else {v} 
                        for k,v in matches.items()}

    @outport
    def out(): pass

    @inport
    def ins(self, data, tag):
        if all(tag[key] in val for key,val in self.matches.items()):
            yield from data >> tag >> self.out

class forward(Unit):
    @outport
    def out(): pass

    @inport
    def ins(self, data, tag):
        yield from data >> tag >> self.out


class tagonly(Unit):
    @outport
    def out(): pass

    @inport
    def ins(self, _, tag):
        yield from None >> tag >> self.out


class Reorder(Paramed, Unit):
    @param
    def by(self, val='path'):
        return val

    @param
    def max(self, val=0):
        return val

    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.orders = []
        self.queued = defaultdict(list)

    @coroutine
    def push(self):
        queued = self.queued
        orders = self.orders
        while orders:
            q = queued[orders[0]]
            if not q:
                break

            load, tag = q.pop(0)
            yield from load >> tag >> self.out

            if tag.flush:
                if queued[orders[0]]:
                    raise
                queued.pop(orders[0])
                orders.pop(0)

    @inport
    def order(self, _, tag):
        self.orders.append(tag[self.by])
        yield from self.push()

    @inport
    def ins(self, load, tag):
        self.queued[tag[self.by]].append((load,tag))
        yield from self.push()


class Collect(Paramed, Unit):
    @param
    def warmup(self, warmup=128):
            return warmup

    @param
    def number(self, number=64):
        """ collect at least this number of packets """
        return number

    @param
    def length(self, length='64k'):
        """ collect until we have at least this length """
        return param.sizeof(length)

    @param
    def timeout(self, timeout=1.0):
        """ wait at most this time before putting out """
        return timeout

    @param
    def max_queued(self, max_queued=8):
        """ maximum number of queued packets """
        return max_queued

    @param
    def collectby(self, collectby=None):
        return collectby

    @param
    def flush(self, value=True):
        return value

    @outport
    def out(self, data, tag):
        pass

    @coroutine
    @withloop
    def __setup__(self, loop=None):
        self.queues = {}

    @coroutine
    def __teardown__(self):
        yield from asyncio.gather([q.put((None, None)) for q in self.queues.values()])

    def reduce(self, datas):
        return sum(datas, [])

    @coroutine
    @withctx
    def pusher(self, q, ctx):
        datas = []
        timeout = self.timeout
        length = self.length
        number = self.number
        total = 0
        count = 0
        tag = port.Tag()
        first = None
        warmup = self.warmup

        reduce = self.reduce
        release = ctx.release

        while True:
            flush = False

            if datas:
                try:
                    data,t = yield from asyncio.wait_for(q.get(), 
                            first + timeout - time.time(), loop=ctx.loop)
                except asyncio.TimeoutError:
                    flush = True
            else:
                data,t = yield from q.get()
                first = time.time()

            if t is None and data is None and self.flush:
                flush = True

            if not flush and t is not None:
                tag.update(t)
                datas.append(data)
                total += len(data)
                count += 1

            if warmup:
                warmup -= 1
                count = number

            if flush or count > number or total > length or t is None or tag.flush:
                if datas:
                    out = reduce(datas)
                    yield from out >> tag.add(
                            collected_num = len(datas),
                            collected_delta = time.time() - first
                            ) >> self.out
                    yield from ctx.release(count)
                    total = count = 0
                    datas=[]
                    tag = port.Tag()

            if not flush and t is None:
                break

    @inport
    @withctx
    def ins(self, data, tag, ctx=None):
        yield from ctx.aquire()

        by = tag.get(self.collectby)
        try:
            q = self.queues[by]
        except KeyError:
            logger.info('CLT.ins new queue for %s', by)
            loop = ctx.loop
            q = asyncio.Queue(self.max_queued, loop=loop)
            asyncio.async(self.pusher(q), loop=loop)
            self.queues[by] = q

        yield from q.put((data, tag))




