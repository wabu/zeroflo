from zeroflo.model.unit import Unit, inport, outport


class Simple(Unit):
    @outport
    def out(): pass

    @inport
    def process(self, data, tag): pass


def test_simple():
    u1 = Simple(name='a')
    u2 = Simple(name='b')
    u3 = Simple(name='c')

    both = u1.__truediv__(u2)

    assert u1.model is u2.model

    assert both.units == [u1, u2]
    assert both.source_ports == [u1.out, u2.out]
    assert both.target_ports == [u1.process, u2.process]

    m1 = u1.model
    con = u1 >> u2

    assert m1 is u2.model

    assert len(u1.model.links()) == 1

    assert con.units == [u1, u2]
    assert con.source_ports == [u2.out]
    assert con.target_ports == [u1.process]

    comb = (u1 / u2) >> u3

    assert len(u1.model.links()) == 3
    assert u3.model == u1.model
    assert comb.units == [u1, u2, u3]

    dist = u1 | u2 & u3

    assert len(u1.model.spaces()) == 2
    assert dist.units == [u1, u2, u3]
