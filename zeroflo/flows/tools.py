from ..core import *
from ..core.packet import Tag
from ..ext import param, Paramed

import time

from collections import defaultdict
from heapq import merge
from pyadds.logging import log

class Timing:
    def __init__(self):
        self.time = self.start = time.time()

    def set(self, now=None):
        if now is None:
            now = time.time()
        self.time = now - self.start

    @property
    def mins(self):
        return int(self.time // 60) % 60

    @property
    def hours(self):
        return int(self.time // 3600)

    @property
    def secs(self):
        return self.time % 60

    def __getitem__(self, name):
        if isinstance(name, str):
            return {'time': self.time, 'mins': self.mins, 'hours': self.hours, 'secs': self.secs}[name]
        elif isinstance(name, int):
            return {-1: self.time, 0: self.secs, 1: self.mins, 2: self.hours}
        elif isinstance(name, tuple):
            return tuple(self[n] for n in name)
        raise KeyError('Timing has no item %s' % name)

    def __iter__(self):
        return iter((self.hours, self.mins, self.secs))

    def __float__(self):
        return self.time

    def __str__(self):
        if self.hours:
            return '{}:{:02d}:{:02.0f}'.format(*self)
        else:
            return '{}:{:04.1f}'.format(*self['mins','secs'])


class Status(Paramed, Unit):
    @param
    def format(self, val='{__tag__!r}'):
        return val

    @param
    def end(self, val='\n'):
        return val

    @coroutine
    def __setup__(self):
        self.timing = Timing()

    @inport
    def process(self, data, tag):
        self.timing.set()
        try:
            length = len(data)
        except Exception:
            length = None
        print(self.format.format(__data__=data,
                                 __len__=length,
                                 __tag__=tag,
                                 __time__=self.timing,
                                 **tag), end=self.end, flush=True)
        yield from []


def ensure(val, typ):
    return val if isinstance(val, typ) else typ([val])


class match(Unit):
    def __init__(self, **matches):
        super().__init__()
        self.matches = {k: ensure(v, set) for k,v in matches.items()}

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        if all(tag[key] in val for key,val in self.matches.items()):
            yield from data >> tag >> self.out


class Categorize(Paramed, Unit):
    @param
    def tag(self, val='category'):
        return val

    @param
    def rules(self, val):
        return [(cat, [{k: ensure(v, set) for k,v in rule.items()}
                       for rule in ensure(rules, list)])
                for cat, rules in val.items()]

    @param
    def default(self, val=None):
        return val

    @outport
    def out(): pass

    @outport
    def non(): pass

    @inport
    def process(self, data, tag):
        for cat, rules in self.rules:
            if any(all(tag[key] in val for key, val in rule.items())
                   for rule in rules):
                break
        else:
            cat = self.default

        if cat is None:
            yield from data >> tag >> self.non
        else:
            yield from data >> tag.add(**{self.tag: cat}) >> self.out


class forward(Unit):
    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from data >> tag >> self.out


class addtag(Unit):
    def __init__(self, **tag):
        super().__init__()
        self.tag = tag

    @outport
    def out(): pass

    @inport
    def process(self, data, tag):
        yield from data >> tag.add(**self.tag) >> self.out


class tagonly(Unit):
    @outport
    def out(): pass

    @inport
    def process(self, _, tag):
        yield from None >> tag >> self.out


class Offset(Unit):
    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.offset = 0

    @inport
    def process(self, data, tag):
        yield from data >> tag.add(offset=offset) >> self.out
        self.offset += len(data)


@log
class Merge(Paramed, Unit):
    @param
    def by(self, val='begin'):
        return val

    @outport
    def out(): pass

    @coroutine
    def __setup__(self):
        self.queue_a = asyncio.JoinableQueue(1)
        self.queue_b = asyncio.JoinableQueue(1)
        asyncio.async(self.loop())

    @coroutine
    def __teardown__(self):
        self.__log.info('tearing down %s', self)
        qs = [self.queue_a, self.queue_b]
        yield from asyncio.gather(q.put((None, None, None)) for q in qs)
        yield from asyncio.gather(q.join() for q in qs)
        self.__log.info('tear down of %s finished', self)

    @coroutine
    def loop(self):
        queue_a, queue_b = self.queue_a, self.queue_b
        fill_a = fill_b = True
        while True:
            if fill_a:
                val_a, data_a, tag_a = yield from queue_a.get()
                fill_a = False
                self.__log.debug('got data form a')
            if fill_b:
                val_b, data_b, tag_b = yield from queue_b.get()
                fill_b = False
                self.__log.debug('got data form b')

            if val_a == val_b:
                data = list(merge(data_a + data_b))
                tag = tag_b.add(**tag_a)
                fill_a = fill_b = True
                self.__log.debug('pushing both')
            elif val_b is None or val_a < val_b:
                data, tag = data_a, tag_a
                fill_a = True
                self.__log.debug('pushing a')
            elif val_a is None or val_b < val_a:
                data, tag = data_b, tag_b
                fill_b = True
                self.__log.debug('pushing b')
            else:
                self.__log.info('shuting down %s', self)
                assert val_a is None and val_b is None
                queue_a.task_done()
                queue_b.task_done()
                break

            yield from data >> tag >> self.out

            if fill_a:
                queue_a.task_done()
            if fill_b:
                queue_b.task_done()


    @inport
    def a(self, data, tag):
        self.__log.debug('received on a')
        yield from self.queue_a.put((tag[self.by], data, tag))

    @inport
    def b(self, data, tag):
        self.__log.debug('received on b')
        yield from self.queue_b.put((tag[self.by], data, tag))


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
    def process(self, load, tag):
        self.queued[tag[self.by]].append((load,tag))
        yield from self.push()


@log
class Collect(Paramed, Unit):
    @param
    def warmup(self, warmup=128):
            return warmup

    @param
    def number(self, number=4):
        """ collect at least this number of packets """
        return number

    @param
    def length(self, length='4k'):
        """ collect until we have at least this length """
        return param.sizeof(length)

    @param
    def timeout(self, timeout=.1):
        """ wait at most this time before putting out """
        return timeout

    @param
    def max_queued(self, max_queued=4):
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
    def __setup__(self):
        self.queues = {}

    @coroutine
    def __teardown__(self):
        yield from asyncio.gather([q.put((None, None)) for q in self.queues.values()])

    def reduce(self, datas):
        return b''.join(datas)

    @coroutine
    def pusher(self, q):
        datas = []
        timeout = self.timeout
        length = self.length
        number = self.number
        total = 0
        count = 0
        tag = Tag()
        first = None
        warmup = self.warmup

        reduce = self.reduce
        #release = ctx.release

        while True:
            flush = False

            if datas:
                try:
                    data,t = yield from asyncio.wait_for(q.get(),
                            first + timeout - time.time())
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
                    #yield from ctx.release(count)
                    total = count = 0
                    datas=[]
                    tag = Tag()

            if not flush and t is None:
                break

    @inport
    def process(self, data, tag):
        #ctx = self.ctx
        #yield from ctx.aquire()

        by = tag.get(self.collectby)
        try:
            q = self.queues[by]
        except KeyError:
            self.__log.info('new queue for %s', by)
            q = asyncio.Queue(self.max_queued)
            asyncio.async(self.pusher(q))
            self.queues[by] = q

        yield from q.put((data, tag))




