import pandas as pd

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
        self.st = pd.HdfStore(h5file, complib=self.complib, complevel=self.complevel)

    @inport
    def process(self, data, tag):
        pass
