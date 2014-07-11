from ..core import *
from ..ext import param, Paramed

import time
import logging
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
            timedout = False

            if datas:
                try:
                    data,t = yield from asyncio.wait_for(q.get(), 
                            first + timeout - time.time(), loop=ctx.loop)
                except asyncio.TimeoutError:
                    timedout = True
            else:
                first = time.time()
                data,t = yield from q.get()

            if not timedout and data is not None:
                tag = tag or t
                datas.append(data)
                total += len(data)
                count += 1

            if warmup:
                warmup -= 1
                count = number

            if timedout or count > number or total > length or data is None:
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

            if not timedout and data is None:
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




