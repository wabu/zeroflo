from asyncio import coroutine
import aiozmq
import asyncio

from .zmqtools import create_zmq_stream
from pyadds.forkbug import maybug

import logging
logger = logging.getLogger(__name__)

class Remote:
    def __init__(self, obj, endpoint):
        self.obj = obj
        self.endpoint = endpoint
        self.address = 'ipc://{}/rpc'.format(endpoint.namespace())
        
    @coroutine
    def __remote__(self):
        with maybug(info=self.obj, namespace=self.endpoint.namespace()):
            self.stream = stream = yield from create_zmq_stream(
                    aiozmq.zmq.REP, bind=self.address)
            obj = self.obj

            while True:
                rq,args,kws = yield from stream.pull()
                logger.debug('remote call to %s' % rq)
                method = getattr(obj, rq)

                result = yield from method(*args, **kws)
                yield from stream.push(result)


    @coroutine
    def __setup__(self):
        self.stream = yield from create_zmq_stream(
                aiozmq.zmq.REQ, connect=self.address)


    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError
        getattr(self.obj, name)

        @coroutine
        def sending(*args, **kws):
            logger.debug('calling remote to %s' % name)
            yield from self.stream.push(name, args, kws)
            return (yield from self.stream.pull())
        setattr(self, name, sending)
        return sending
        




