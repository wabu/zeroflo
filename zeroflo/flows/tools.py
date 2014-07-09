from ..core import *
from ..ext import param, Paramed

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

    @outport
    def out(self, data, tag):
        pass

    @coroutine
    @withloop
    def __setup__(self, loop=None):
        self.q = asyncio.Queue(self.max_queued, loop=loop)
        self.push = asyncio.async(self.pusher(), loop=loop)

    @coroutine
    def __teardown__(self):
        yield from self.q.put((None, None))
        yield from self.push.wait()

    def reduce(self, datas):
        return sum(datas, [])

    @coroutine
    @withctx
    def pusher(self, ctx):
        q = self.q
        datas = []
        timeout = self.timeout
        length = self.length
        number = self.number
        total = 0
        count = 0

        reduce = self.reduce
        release = ctx.release

        while True:
            timedout = False

            if datas:
                try:
                    data,tag = yield from asyncio.wait_for(q.get(), timeout, loop=ctx.loop)
                except asyncio.TimeoutError:
                    timedout = True
            else:
                data,tag = yield from q.get()

            if not timedout and data is not None:
                datas.append(data)
                total += len(data)
                count += 1

            if timedout or count > number or total > length:
                if datas:
                    data = reduce(datas)
                    yield from data >> tag >> self.out
                    for _ in range(count):
                        yield from ctx.release()
                    total = count = 0
                    datas=[]

            if not timedout and data is None:
                break

    @inport
    @withctx
    def ins(self, data, tag, ctx=None):
        yield from ctx.aquire()
        yield from self.q.put((data, tag))
