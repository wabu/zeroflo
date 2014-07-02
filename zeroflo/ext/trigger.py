from ..core import *

import logging

logger = logging.getLogger(__name__)

class Trigger(Flo):
    @outport
    def trig(): pass

    @inport
    def once(self, data, tag):
        logger.debug('triggering %s once with %s', self, data >> tag)
        yield from data >> tag >> self.trig
