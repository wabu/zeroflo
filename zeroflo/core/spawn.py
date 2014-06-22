import multiprocessing as mp
import asyncio
import aiozmq

import logging
logger = logging.getLogger(__name__)

def call_entry(proc):
    logger.info('spawned %s', proc)
    try:
        logger.debug('calling %s', proc.__entry__)
        proc.__entry__()
        logger.debug('exited %s', proc)
    except Exception as e:
        logger.error('execpt %s', proc, exc_info=True)

class MPSpawner:
    def __init__(self):
        mp.set_start_method('forkserver')

    @asyncio.coroutine
    def spawn(self, proc):
        logger.debug('%s spawning %s', self, proc)
        proc = mp.Process(target=call_entry, args=(proc,))
        proc.start()

