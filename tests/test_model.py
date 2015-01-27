import pytest

from zeroflo.model.unit import Unit, inport, outport


class Simple(Unit):
    @outport
    def out(self):
        """
        outport for no data
        """

    @inport
    def process(self, data, tag):
        """
        :param data: data of a packet
        :param tag: tag for the data
        """

@pytest.fixture
def a():
    return Simple(name='a')

@pytest.fixture
def b():
    return Simple(name='a')

@pytest.fixture
def c():
    return Simple(name='a')

def test_model(a, b):
    assert a.model != b.model

    assert a in a.model.units()
    assert b in b.model.units()

    a.model.join(b)
    # FIXME do we require a unification for model functions?
    #assert a.model == b.model


def test_dsl(a, b, c):
    both = a.__truediv__(b)

    assert a.model is b.model

    assert both.units == [a, b]
    assert both.source_ports == [a.out, b.out]
    assert both.target_ports == [a.process, b.process]

    m1 = a.model
    con = a >> b

    assert m1 is b.model

    assert len(a.model.links()) == 1

    assert con.units == [a, b]
    assert con.source_ports == [b.out]
    assert con.target_ports == [a.process]

    comb = (a / b) >> c

    assert len(a.model.links()) == 3
    assert c.model == a.model
    assert comb.units == [a, b, c]

    dist = a | b & c

    assert len(a.model.spaces()) == 2
    assert dist.units == [a, b, c]


    assert a | c
    with pytest.raises(IndexError):
        a & c

    assert b & c
    with pytest.raises(IndexError):
        b | c

    assert len(a.model.spaces()) == 2

    direct = c.out >> b.process

    assert direct.model == c.model
    assert len(c.model.links()) == 4
    assert len(c.model.links(sources={c}, targets={b})) == 1
