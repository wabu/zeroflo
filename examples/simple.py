import logging
import zeroflo as flo

import os

class Source(flo.Unit):
    @flo.inport
    def process(self, datas, tag):
        print('<<', datas)
        for data in datas:
            print(os.getpid(), ' <', data)
            yield from data >> tag >> self.out

    @flo.outport
    def out(): pass

class Process(flo.Unit):
    @flo.inport
    def process(self, data, tag):
        print(os.getpid(), ' !', data)
        yield from data * 2 >> tag.add(doubled=True) >> self.out

    @flo.outport
    def out(): pass


class Sink(flo.Unit):
    @flo.inport
    def process(self, data, tag):
        print(os.getpid(), '>>', data)


import asyncio
import aiozmq

def setup_logging():
    logging.basicConfig(format='%(asctime)s %(levelname)-7s%(processName)16s|%(name)-24s\t%(message)s')
    logging.getLogger('zeroflo').setLevel("DEBUG")
    #logging.getLogger('zeroflo.tools').setLevel("DEBUG")
    logging.getLogger('zeroflo.core.flow').setLevel("DEBUG")
    logging.getLogger('zeroflo.core.zmqtools').setLevel("INFO")

if __name__ == "__main__":
    from examples.simple import *

    ctx = flo.Context('simple')

    with ctx.setup(setup=setup_logging):
        # create flow units
        src = Source()
        prc = Process()
        snk = Sink()

        # connect flow units
        src >> prc >> snk

        src & snk | prc

        print('--')
        print(repr(ctx.topology))

    with ctx.run():
        src.process(['one', 'two', 'three'])
        src.process(range(9))

        src.process.delay('abc')
        src.process.delay(range(3))
        for i,tag in prc.out:
            print('###', i, tag, '###')

        print('---', ' ', '---')
        snk.join()

    print('done')

