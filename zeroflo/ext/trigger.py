from ..core import *

import logging

logger = logging.getLogger(__name__)

class Trigger(Unit):
    @outport
    def out(): pass

    @inport
    def once(self, data, tag):
        logger.info('triggering %s once with %s', self, data >> tag)
        yield from data >> tag >> self.out

    @inport
    def each(self, items, tag):
        logger.info('triggering %s each in %s', self, items >> tag)
        for item in items:
            yield from item >> tag >> self.out
