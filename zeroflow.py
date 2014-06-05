from . import flo 
from .zerotools import *
from .flo import *
from .tools import nids

import multiprocessing as mp

import logging
logger = logging.getLogger(__name__)

class ZmqPolicy(FlowPolicy):
    def new_flow_context(self, name=None):
        mp.set_start_method('forkserver')
        asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
        return ZmqFlow(name)

flo._init_flow_policy = ZmqPolicy

class ZmqSpace(Space, ZmqProcess):
    def __getstate__(self):
        state = super().__getstate__()
        state['_recv'] = None
        state['_send'] = None
        return state

    @remote
    def connect_flow(self, address):
        logger.debug('%s connects to flow on %s', self, address)
        yield from self.ctx.setup_interfacing(address)

    @property
    def inside(self):
        return self.dispatching

    @coroutine
    def _handling(self, address):
        logger.debug('%s binds to %s', self, address)
        stream = yield from create_zmq_stream(zmq.DEALER, bind=address)
        while True:
            logger.debug('%s pulls from %s', self, stream)
            nid, port, packet = yield from stream.pull()
            logger.debug('%s pulled %s, %s, %s', self, nid, port, packet)
            try:
                yield from self.handle(nid, port, packet)
            except Exception as e:
                logger.error('%s raised %s on (%d, %s, %s)', self, e, nid or -1, port, packet)
                raise e
        logger.debug('%s shuts down', self)
        stream.close()

    @remote
    def setup_handler(self, address):
        logger.debug('%s requested to handle on %s', self, address)
        asyncio.async(self._handling(address))

    @remote
    def register(self, fl):
        yield from super().register(fl)

    @register.after
    def register(self, fl):
        yield from super().register(fl)


class ZmqLink(Link):
    def __init__(self, ctx, source, target):
        super().__init__(ctx, source, target)
        self.stream = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state['stream'] = None
        return state
    
    @coroutine
    def handle(self, packet):
        logger.debug('%s handling %s', self, packet)
        target = self.target
        stream = self.stream
        if stream is None:
            logger.debug('%s is setting up', self)
            space = yield from self.ctx.space_of(target.fl)

            address = self.ctx.address(space.nid, target.fl.name, 'ins')
            yield from space.setup_handler(address)
            stream = yield from create_zmq_stream(zmq.DEALER, connect=address)
            logger.debug('%s setup with %s to %s', self, stream, space)
            self.stream = stream

        yield from self.stream.push(target.fl.nid, target.name, packet)


class ZmqFlow(ZmqRemote, Context):
    # perpahps sync registers ...

    def address(self, *subs):
        subs = (self.nid,) + subs
        base = '/'.join(subs[:-1])
        try:
            os.makedirs(base)
        except FileExistsError:
            pass
        return 'ipc://{}/{}'.format(base, subs[-1])

    @coroutine
    def setup_space(self, fls):
        if not self.interfacing and not self.dispatching:
            logger.debug('%s starting ...', self)
            yield from self.setup_dispatching(self.address('ctrl'))
        
        logger.debug('%s getting space for %s', self, fls)
        space = yield from self.ensure_space(nids(fls))
        yield from space.setup_interfacing(self.address(space.nid, 'ctrl'))
        return space

    @remote
    def ensure_space(self, nids):
        logger.debug('%s ensures space for %s', self, nids)
        fls = {self.fls[nid] for nid in nids}
        for fl in fls:
            if fl.space:
                space = fl.space
        else:
            space = yield from self.spawn_space(fls)

        return space

    @coroutine
    def spawn_space(self, fls):
        space = ZmqSpace(fls, ctx=self)
        logger.debug('%s spawning %s', self, space)
        yield from space.setup(self.address(space.nid, 'ctrl'))
        yield from space.connect_flow(self.address('ctrl'))
        return space

    def mk_link(self, kind, source, target):
        if kind == 'remote':
            return ZmqLink(self, source, target)
        else:
            return super().mk_link(kind, source, target)
