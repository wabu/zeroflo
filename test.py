import logging


from datalibs.util import delayed
from zeroflo.flo import *
from zeroflo.zeroflow import *

import os

class Range(Unit):
    @outport
    def outs(i : int, tag : {'n': int}):
        """"""

    @inport
    def range(self, n : int, tag : {}):
        for i in range(n):
            yield from i >> tag.add(n=n) >> self.outs
        

class Cumulate(Unit):
    @delayed
    def current(self):
        return 0

    @outport
    def cums(c : int, tag : {}):
        """"""

    @inport
    def ins(self, k : int, tag : {}):
        self.current += k
        yield from self.current >> tag >> self.cums


class Print(Unit):
    @inport
    def print(self, o : ..., tag : {}):
        print('>>', os.getpid(), o)


if __name__ == "__main__":
    logging.basicConfig(format='[%(process)d] %(levelname)s %(message)s')

    logger = logging.getLogger('zeroflo')
    logger.setLevel("DEBUG")
    #logger.addHandler(logging.StreamHandler())

    rng = Range()
    cum = Cumulate()
    prt = Print()

    rng | cum | prt

    rng.outs >> cum.ins
    cum.cums >> prt.print

    #flow = rng | prt
    #rng.outs >> prt.print

    rng.range(10)
