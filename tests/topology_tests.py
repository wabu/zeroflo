import unittest
from collections import namedtuple

from zeroflo.top.topology import Topology
from zeroflo.top.dsl import UnitDSL, SourceDSL, TargetDSL

Prt = namedtuple('Port', 'name, unit')


class TopologyTestCase(unittest.TestCase):
    def setUp(self):
        self.top = Topology()

    def test_register(self):
        self.top.register('a')
        self.top.register('b')
        self.top.register('c')

        self.assertEqual(set(self.top.units), {'a', 'b', 'c'})

        self.top.unregister('b')
        self.assertEqual(set(self.top.units), {'a', 'c'})

    def test_join_part(self):
        a = self.top.register('a')
        b = self.top.register('b')
        c = self.top.register('c')

        self.top.join('a', 'b')
        self.top.part('b', 'c')

        self.assertEqual(len(self.top.spaces), 2)
        self.assertIn({a, b}, self.top.spaces)
        self.assertIn({c}, self.top.spaces)

    def test_link(self):
        self.top.register('a')
        self.top.register('b')
        self.top.register('c')

        aout = Prt('aout', 'a')
        bins = Prt('bins', 'b')
        bout = Prt('bout', 'b')
        cins = Prt('cins', 'c')

        atob = self.top.link(aout, bins)
        btoc = self.top.link(bout, cins)

        self.assertEqual(len(self.top.srcports), 2)
        self.assertEqual(len(self.top.tgtports), 2)
        self.assertEqual(len(self.top.links), 2)

        self.assertEqual(self.top.srcports[atob.sid], aout)
        self.assertEqual(self.top.tgtports[atob.tid], bins)

        self.assertEqual(self.top.srcports[btoc.sid], bout)
        self.assertEqual(self.top.tgtports[btoc.tid], cins)

        self.top.unlink(aout, bins)
        self.assertEqual(len(self.top.links), 1)


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


class DSLTestCase(unittest.TestCase):
    def test_simple(self):
        top = Topology()
        u1 = Unit(top, 'u1')
        u2 = Unit(top, 'u2')

        self.assertEqual(u1.dsl.left, u1)
        self.assertEqual(u2.dsl.right, u2)

        self.assertEqual(u1.dsl.sources, [u1.out])
        self.assertEqual(u2.dsl.targets, [u2.process])
