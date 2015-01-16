import pytest

from collections import namedtuple

from zeroflo.top.topology import Topology
from zeroflo.top.dsl import UnitDSL, SourceDSL, TargetDSL


Prt = namedtuple('Port', 'name, unit')


def mk_top(items=['a', 'b', 'c']):
    top = Topology()
    return top, list(map(top.register, items))


def test_register():
    top, _ = mk_top()

    assert set(top.units) == {'a', 'b', 'c'}

    top.unregister('b')
    assert set(top.units) == {'a', 'c'}


def test_join_part():
    top, (a, b, c) = mk_top()

    top.join('a', 'b')
    top.par('b', 'c')

    assert top.space_of('a') == top.space_of('b')
    assert {'a', 'b'} == top.space_of('b').units
    assert {'c'} == top.space_of('c').units
    assert len(top.spaces) == 2

    with pytest.raises(IndexError):
        top.par('a', 'b')
    with pytest.raises(IndexError):
        top.join('b', 'c')

    top.join('d')
    assert len(top.spaces) == 3

    top.join('b', 'd')
    assert len(top.spaces) == 2

    top.unregister('c')
    assert len(top.spaces) == 1

    top.register('c')
    top.join('b', 'c')
    assert len(top.spaces) == 1


def test_link():
    top, _ = mk_top()

    aout = Prt('aout', 'a')
    bins = Prt('bins', 'b')
    bout = Prt('bout', 'b')
    cins = Prt('cins', 'c')

    atob = top.link(aout, bins)
    btoc = top.link(bout, cins)

    assert len(top.links) == 2

    assert top.links_from('a') == {atob}
    assert top.links_to('b') == {atob}

    assert top.links_from('b') == {btoc}
    assert top.links_to('c') == {btoc}

    top.unlink(atob)
    assert len(top.links) == 1

    top.link(aout, bins)
    assert len(top.links) == 2
    top.unregister('b')
    assert len(top.links) == 0


def test_bundle():
    top, _ = mk_top()

    b1 = top.bundle({'a', 'b'}, foo=1)
    b2 = top.bundle({'c'}, bar=2)
    b3 = top.bundle({'a', 'c'}, foo=3)
    b4 = top.bundle({'a', 'b', 'c'}, baz=4)

    assert len(top.bundles) == 4
    assert b1.opts == {'foo': 1}
    assert b2.opts == {'bar': 2}
    assert b3.opts == {'foo': 3}
    assert b4.opts == {'baz': 4}

    top.unregister('c')
    assert len(top.bundles) == 3

    b5 = top.bundle({'a', 'b'}, baz=3)
    assert b5.opts == {'baz': 3}


class Port:
    def __init__(self, name, unit, model):
        super().__init__(model)
        self.name = name
        self.unit = unit

    def __str__(self):
        return '{}.{}'.format(self.unit, self.name)

    __repr__ = __str__


class Ins(Port, TargetDSL):
    pass


class Out(Port, SourceDSL):
    pass


class Unit(UnitDSL):
    def __init__(self, model, name, ins=['process'], outs=['out']):
        super().__init__(model=model)
        self.name = name

        for prt in ins:
            setattr(self, prt, Ins(prt, self, model))
        for prt in outs:
            setattr(self, prt, Out(prt, self, model))

    def __str__(self):
        return self.name

    __repr__ = __str__


def test_simple():
    top = Topology()

    u1 = Unit(top, 'u1')
    u2 = Unit(top, 'u2')
    u3 = Unit(top, 'u3')
    u4 = Unit(top, 'u4')

    assert (u1 + u2).u1 == u1
    assert (u1 / u2).u2 == u2

    with pytest.raises(AttributeError):
        (u1 / u2).u3

    with pytest.raises(AttributeError):
        (u1 + Unit(top, 'u1')).u1

    assert u1.dsl.left == u1
    assert u2.dsl.right == u2

    assert u2.dsl.targets == [u2.process]
    assert u3.dsl.sources == [u3.out]

    d1 = u1.out >> u2.process
    assert len(top.links) == 1

    assert d1.units == (u1, u2)
    assert d1.left == u1
    assert d1.right == u2
    assert d1.sources == {}
    assert d1.targets == {}

    assert str(d1) == '|{},{}|'.format(u1, u2)

    d2 = u2 >> u3
    assert len(top.links) == 2

    assert d2.units == (u2, u3)
    assert d2.sources == [u3.out]
    assert d2.targets == [u2.process]

    assert (repr(d2) ==
            '|{},{}| <<{} >>{}'.format(u2, u3, u2.process, u3.out))

    d3 = (u1 ^ 'baz' | u4) ** 4 | (u2 * 2 & u3)

    assert d3.sources == [u3.out]
    assert set(d3) == {u1, u2, u3, u4}

    assert len(top.spaces) == 3
    assert {u1} == top.space_of(u1).units
    assert {u2, u3} == top.space_of(u2).units
    assert {u2, u3} == top.space_of(u3).units
    assert {u4} == top.space_of(u4).units

    d4 = (u2 / u3) >> u4
    assert len(top.links) == 4
    assert d4.units == (u2, u3, u4)
    assert d4.targets == [u2.process, u3.process]

    assert len(top.bundles) == 3
    # TODO ... further bundle spec
