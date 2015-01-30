import pytest

from zeroflo.model.objects import Unit, inport, outport
from zeroflo import context


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
def model():
    return context.mk_model()


@pytest.fixture
def a():
    return Simple(name='unit-a')


@pytest.fixture
def b():
    return Simple(name='unit-b')


@pytest.fixture
def c():
    return Simple(name='unit-c')


@pytest.fixture
def d():
    return Simple(name='unit-d')


def test_model(model, a, b):
    model.register(a, b)
    assert len(model.units()) == 2
    assert a in model.units()
    assert b in model.units()

    assert a.model == model
    assert b.model == model

    model.join(a, b)
    assert len(model.spaces()) == 1

    assert model.instance == model
    assert model != a

    assert 'unit-a' in str(model)
    assert 'unit-a' in repr(model)

    model.unregister(b)
    assert b not in model.units()
    assert b.model != model


def test_unify(a, b, c):
    m1 = context.mk_model()
    m2 = context.mk_model()

    m1.register(a, b)
    m2.register(c)

    assert len(m1.units()) == 2
    assert len(m2.units()) == 1

    assert m1 != m2

    m1.register(c)

    assert m1 == m2
    assert len(m2.units()) == 3


def test_links(model, a, b, c):
    model.register(a, b, c)

    model.link(a.out, b.process)
    assert len(model.links()) == 1
    model.link(b.out, a.process)
    assert len(model.links()) == 2
    model.link(a.out, c.process, queue=42)
    assert len(model.links()) == 3

    model.unlink(b, a)
    assert len(model.links()) == 2

    # unlink non-exisiting
    model.unlink(b, a)
    assert len(model.links()) == 2

    # double link
    model.link(a.out, b.process)
    assert len(model.links()) == 3
    # double unlink
    model.unlink(a, b)
    assert len(model.links()) == 1

    link, = model.links()
    assert 'a.out' in str(link)
    assert 'c.process' in repr(link)
    assert '42' in repr(link)

    model.unregister(c)
    assert c not in model.units()
    assert len(model.links()) == 0


def test_spaces(model, a, b, c, d):
    model.register(a, b, c, d)
    model.join(a, b)
    model.par(b, c)

    assert len(model.spaces()) == 2

    with pytest.raises(IndexError):
        model.join(b, c)

    with pytest.raises(IndexError):
        model.par(a, b)

    model.join(c, d)
    model.par(a, d)
    assert len(model.spaces()) == 2

    model.unregister(c)
    model.unregister(d)

    assert len(model.spaces()) == 1

    model.register(c, d)
    model.join(c)
    model.join(d)

    model.join(a, c, d)
    assert len(model.spaces()) == 1

    space, = model.spaces()

    assert 'unit-a' in str(space)
    assert 'unit-d' in repr(space)


def test_dsl(a, b, c):
    # use method as op hides errors under NotImplemented
    both = a.__truediv__(b)

    assert a.model == b.model

    assert both.units == [a, b]
    assert both.source_ports == [a.out, b.out]
    assert both.target_ports == [a.process, b.process]

    m1 = a.model
    con = a >> b

    assert m1 == b.model

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
