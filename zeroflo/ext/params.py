"""
parameter handling for flow units ...
"""
from pyadds.annotate import *
from ..core.unit import inport

import logging
logger = logging.getLogger(__name__)

_size_suffixes = ['k', 'm', 'g', 't', 'p']
class param(Annotate, Cached, ObjDescr, Get, Set):
    @staticmethod
    def sizeof(size):
        try:
            size = int(size)
        except ValueError:
            pass

        if isinstance(size, str):
            suf = size[-1]
            k = _size_suffixes.index(suf.lower()) + 1
            size = int(size[:-1]) * 1024**k

        if not isinstance(size, int):
            raise TypeError('size should be an <int><suff?> with suff=K|M|G|T')

        return size

class Paramed:
    def __init__(self, *args, **kws):
        self.set_params(kws)
        super().__init__(*args, **kws)

    def set_params(self, kws):
        for par in param.iter(self):
            if par.name in kws:
                val = kws.pop(par.name)
                logger.debug('param %s=%s', par.name, val)
                par.__set__(self, par.definition(self, val))

    @inport
    def setup(self, _, **kws):
        self.set_params(kws)

