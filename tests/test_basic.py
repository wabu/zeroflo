import zeroflo as flo

import pytest


class Range(flo.Unit):
    @flo.outport
    def out(self):
        """ outport getting a range of numbers """

    @flo.inport
    def process(self, n, tag):
        for i in range(n):
            yield from i >> tag >> self.out


class Cum(flo.Unit):
    @flo.coroutine
    def __setup__(self):
        self.cum = 0

    @flo.outport
    def out(self):
        """ outport getting cumulated items """

    @flo.inport
    def process(self, i, tag):
        self.cum += i
        yield from self.cum >> tag >> self.out


def test_compat():
    ctx = flo.Context('test')

    with ctx.setup():
        rng = Range()
        cum = Cum()
        rng >> cum

    with ctx.run():
        rng.process.delay(5)
        items = [data for data, tag in cum.out]

        ctx.control.shutdown()

    assert items == list(sum(range(i+1)) for i in range(5))


def test_simple():
    pytest.skip('TODO new flo')

    flow = Range() >> Cum()

    items = list(flow.process(5))
    assert items == list(sum(range(i)) for i in range(5))


def test_opts():
    pytest.skip('TODO new flo')
    flow = Range() >> Cum()
    flow.range & flow.cum

    items = list(flow.process(5))
    assert items == list(sum(range(i)) for i in range(5))


def test_anonymous():
    pytest.skip('TODO new flo')
    flow = Range() >> (lambda x: x*2) >> Cum()

    items = list(flow.process(5))
    assert items == list(sum(range(0, i, 2) for i in range(5)))


def test_named():
    pytest.skip('TODO new flo')
    flow = Range(name='rng') >> Cum(name='sum')
    flow.rng | flow.sum

    items = list(flow.process(5))
    assert items == list(sum(range(i)) for i in range(5))


def test_varz():
    pytest.skip('TODO new flo')
    flow = rng, cum = Range() >> Cum()
    rng & cum

    items = list(flow.process(5))
    assert items == list(sum(range(i)) for i in range(5))

def test_vars():
    pytest.skip('TODO new flow')

    rng = Range()
    cum = Cum()

    (rng >> cum,
     rng & cum)

    rng.process(5)
    items = list(cum.out)

    assert items == list(sum(range(i)) for i in range(5))
