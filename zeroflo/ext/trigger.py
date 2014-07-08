from ..core import *

import logging

logger = logging.getLogger(__name__)

class Trigger(Unit):
    @outport
    def out(): pass

    @inport
    def once(self, data, tag):
        logger.debug('triggering %s once with %s', self, data >> tag)
        yield from data >> tag >> self.out
