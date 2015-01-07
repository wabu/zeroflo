import pandas as pd

from zeroflo import param, coroutine, inport


class H5:
    @param
    def h5file(self, h5file):
        return h5file

    @param
    def complib(self, complib='blosc'):
        return complib

    @param
    def complevel(self, complevel=6):
        return complevel

    @coroutine
    def __setup__(self):
        self.st = pd.HdfStore(self.h5file,
                              complib=self.complib, complevel=self.complevel)

    @inport
    def process(self, data, tag):
        pass
