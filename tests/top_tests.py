import unittest

from zeroflo.top import container
from collections import namedtuple


class GenTestCase(unittest.TestCase):
    def do_it(self, gen, a=4, b=2, c=4):
        ids = set()
        seen = set()
        for i in range(a):
            id = gen.next()
            self.assertNotIn(id, ids)
            ids.add(id)
            seen.add(id)

        for i in range(b):
            id = ids.pop()
            gen.free(id)

        for i in range(c):
            id = gen.next()
            self.assertNotIn(id, ids)
            ids.add(id)
            seen.add(id)
        return seen

    def test_simple_gen(self):
        seen = self.do_it(container.IdGen())
        self.assertEquals(len(seen), 8)

    def test_reusing_gen(self):
        seen = self.do_it(container.ReusingGen())
        self.assertEquals(len(seen), 6)


class ContainerTestCase(unittest.TestCase):
    elems = ['a', 'b', 'c', 'd', 'e']
    Cont = container.Container

    def setUp(self):
        self.a, self.b, self.c, self.d, self.e = self.elems

    def test_unique(self):
        cont = self.Cont()

        r1 = cont.add(self.a)
        r2 = cont.add(self.a)
        self.assertEqual(r1, r2)

        cont.add(self.b)
        cont.add(self.c)
        r3 = cont.add(self.a)
        self.assertEqual(r1, r3)

    def test_differ(self):
        cont = self.Cont()
        r1 = cont.add(self.a)
        r2 = cont.add(self.b)
        self.assertNotEqual(r1, r2)

        cont.add(self.a)
        r3 = cont.add(self.c)
        self.assertNotEqual(r1, r3)
        self.assertNotEqual(r2, r3)

    def test_lookup(self):
        cont = self.Cont()

        r1 = cont.add(self.a)
        i1 = cont[r1]
        self.assertEqual(i1, self.a)

        r2 = cont.add(self.b)
        i2 = cont[r2]
        i1 = cont[r1]

        self.assertEqual(i2, self.b)
        self.assertEqual(i1, self.a)

    def test_remove(self):
        cont = self.Cont()
        r1 = cont.add(self.a)
        r2 = cont.remove(self.a)

        self.assertRaises(KeyError, cont.index, self.a)
        self.assertEqual(r1, r2)

        r3 = cont.add(self.a)
        self.assertNotEqual(r1, r3)

        del cont[r3]
        self.assertRaises(KeyError, cont.index, self.a)

    def test_many(self):
        cont = self.Cont()
        steps = [+1, +2, +3, +4, -2, -1, +2, +5, -4, +2, +3, +1, -2, +1, -5]

        active = set()
        ids = set()

        for step in steps:
            if step > 0:
                elem = self.elems[step-1]
                r = cont.add(elem)
                if elem in active:
                    self.assertIn(r, ids)
                else:
                    self.assertNotIn(r, ids)
                    ids.add(r)
                    active.add(elem)
            else:
                elem = self.elems[-step-1]
                r = cont.remove(elem)
                active.remove(elem)
                ids.remove(r)

            self.assertEqual(set(cont.values()), set(active))
            self.assertEqual(set(cont.keys()), set(ids))

    def test_mulit(self):
        a = self.Cont()
        b = self.Cont()

        ra = a.add(self.a)
        rb = b.add(self.a)

        self.assertNotEqual(ra, rb)

    def test_container(self):
        cont = self.Cont()
        self.assertEquals(len(cont), 0)

        r1 = cont.add(self.a)
        self.assertEquals(len(cont), 1)

        r2 = cont.add(self.b)
        self.assertEquals(len(cont), 2)

        self.assertTrue(self.a in cont)
        self.assertTrue(self.b in cont)
        self.assertFalse(self.c in cont)

        self.assertEqual(set(cont), {self.a, self.b})
        self.assertEqual(cont[r1], self.a)
        self.assertEqual(cont[r2], self.b)

        cont.add(self.a)
        self.assertEquals(len(cont), 2)

        cont.remove(self.a)
        self.assertEquals(len(cont), 1)

        cont.add(self.c)
        self.assertEquals(len(cont), 2)

    def test_setitem(self):
        cont = self.Cont()
        r1 = cont.add(self.a)
        r2 = cont.add(self.b)

        cont[4] = self.c

        self.assertIn(self.c, cont)
        self.assertEqual(cont[4], self.c)
        self.assertEqual(cont.index(self.c).id, 4)

        cont[r1.id] = self.a
        self.assertEqual(r1, cont.index(self.a))
        self.assertRaises(ValueError, cont.__setitem__, r2.id, self.d)


class ReferedContainerTestCase(ContainerTestCase):
    Cont = container.ReferedContainer

    def test_container(self):
        cont = self.Cont()

        a = cont.add(self.a, 0)
        b = cont.add(self.b, 0)

        c = cont.add(self.c, 1)

        self.assertEquals(set(cont.refered_values(0)), {self.a, self.b})
        self.assertEquals(set(cont.refered_values(1)), {self.c})

        self.assertEquals(set(cont.refered_keys(0)), {a, b})
        self.assertEquals(set(cont.refered_keys(1)), {c})

        cont.dismiss(0)

        self.assertEqual(set(cont), {self.c})


Cons = namedtuple('Cons', 'car, cdr')


class AssocContainerTestCase(ContainerTestCase):
    Cont = container.AssocContainer
    elems = [Cons('a', 'b'), Cons('b', 'c'), Cons('d', 'e'),
             Cons('e', 'f'), Cons('f', 'g')]

    def test_assoc(self):
        cont = self.Cont()

        ab = cont.add(Cons('a', 'b'))
        ac = cont.add(Cons('a', 'c'))
        dc = cont.add(Cons('d', 'c'))
        da = cont.add(Cons('d', 'a'))

        self.assertEqual(set(cont.refered_keys(car='a')), {ab, ac})
        self.assertEqual(set(cont.refered_keys(cdr='a')), {da})
        self.assertEqual(set(cont.refered_keys(cdr='c')), {ac, dc})
        self.assertEqual(set(cont.refered_keys(car='a', cdr='c')), {ac})

        cont.dismiss(car='a')

        self.assertEqual(set(cont), {Cons('d', 'c'), Cons('d', 'a')})


class SetsContainerTestCase(ContainerTestCase):
    Cont = container.SetsContainer
    elems = [('a',), ('b',), ('f',), ('c',), ('d',)]

    def test_sets(self):
        cont = self.Cont()

        ab = cont.add({'a', 'b'})
        bc = cont.add({'b', 'c'})
        cont.add({'d', 'e'})

        self.assertEqual(set(cont.refered_keys('b')), {ab, bc})
        self.assertEqual(next(iter(cont.refered_values('e'))), {'d', 'e'})


class GroupsTestCase(SetsContainerTestCase):
    Cont = container.GroupedContainer

    def test_groups(self):
        cont = self.Cont()

        cont.join('a', 'b', 'c')
        cont.join('d', 'e')
        cont.part('e', 'f')
        cont.part('c', 'e')
        cont.join('f', 'g')
        cont.part('h', 'b')

        self.assertIn({'a', 'b', 'c'}, cont)
        self.assertIn({'d', 'e'}, cont)
        self.assertIn({'f', 'g'}, cont)
        self.assertIn({'h'}, cont)


if __name__ == '__main__':
    unittest.main()
