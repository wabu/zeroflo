from .flow import *

import logging
logging.basicConfig(format='[%(process)d] %(levelname)5s %(message)s')
logging.getLogger('zeroflo').setLevel("INFO")

class Trigger(Unit):
    @outport
    def out(): pass

    @inport
    def once(self, data, tag):
        yield from data >> tag >> self.out

    __call__ = once
