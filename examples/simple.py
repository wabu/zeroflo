import logging
import zeroflo as flo

class Source(flo.Unit):
    @flo.inport
    def ins(self, datas, tag):
        print('<<', datas)
        for data in datas:
            print(' <', data)
            yield from data >> tag >> self.out

    @flo.outport
    def out(): pass

class Process(flo.Unit):
    @flo.inport
    def ins(self, data, tag):
        print(' !', data)
        yield from data * 2 >> tag.add(doubled=True) >> self.out

    @flo.outport
    def out(): pass


class Sink(flo.Unit):
    @flo.inport
    def ins(self, data, tag):
        print('>>', data)


def setup_logging():
    logging.basicConfig(format='[%(process)d] %(levelname)5s %(message)s')
    logging.getLogger('zeroflo').setLevel("INFO")
    #logging.getLogger('zeroflo.tools').setLevel("DEBUG")
    #logging.getLogger('zeroflo.core.flow').setLevel("DEBUG")

if __name__ == "__main__":
    from examples.simple import *

    with flo.context('simple', setup=setup_logging) as ctx:
        # create flow units
        src = Source()
        prc = Process()
        snk = Sink()

        # connect flow units
        src.out >> prc.ins
        prc.out >> snk.ins


        # specifiy distribution
        src | prc & snk

    # simple call to trigger flow
    src.ins(['one', 'two', 'three'])
    src.ins(range(9))
