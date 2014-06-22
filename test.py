import logging

import zeroflo as flo
import os

class Range(flo.Unit):
    @flo.outport
    def outs(i : int, tag : {'n': int}):
        """"""

    @flo.inport
    def range(self, n : int, tag : {}):
        for i in range(n):
            yield from i >> tag.add(n=n) >> self.outs
        

class Cumulate(flo.Unit):
    @flo.local
    def current(self):
        return 0

    @flo.outport
    def cums(c : int, tag : {}):
        """"""

    @flo.inport
    def ins(self, k : int, tag : {}):
        self.current += k
        yield from self.current >> tag >> self.cums


class Print(flo.Unit):
    @flo.inport
    def print(self, o : ..., tag : {}):
        print('>>', os.getpid(), o)


def setup_logging():
    logging.basicConfig(format='[%(process)d] %(levelname)5s %(message)s')
    logging.getLogger('zeroflo').setLevel("DEBUG")

if __name__ == "__main__":
    with flo.context('testflo', setup=setup_logging) as ctx:

        # create flow units
        rng = Range()
        cum = Cumulate()
        prt = Print()

        # connect flow units
        rng.outs >> cum.ins
        cum.cums >> prt.print

        # specify distribution of units
        rng | cum & prt

    # simple call to trigger flow
    rng.range(10)

