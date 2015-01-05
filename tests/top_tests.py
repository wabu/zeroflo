__author__ = 'dwae'

import unittest
from zeroflo.top.refer import Container
from zeroflo.top.refs import IdGen, ReusingGen


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
        seen = self.do_it(IdGen())
        self.assertEquals(len(seen), 8)

    def test_reusing_gen(self):
        seen = self.do_it(ReusingGen())
        self.assertEquals(len(seen), 6)


class ContainerTestCase(unittest.TestCase):
    def test_unique(self):
        cont = Container()

        r1 = cont.register('a')
        r2 = cont.register('a')
        self.assertEqual(r1, r2)

        cont.register('b')
        cont.register('c')
        r3 = cont.register('a')
        self.assertEqual(r1, r3)

    def test_differ(self):
        cont = Container()

        r1 = cont.register('a')
        r2 = cont.register('b')
        self.assertNotEqual(r1, r2)

        cont.register('a')
        r3 = cont.register('c')
        self.assertNotEqual(r1, r3)
        self.assertNotEqual(r2, r3)

    def test_lookup(self):
        cont = Container()

        r1 = cont.register('a')
        i1 = cont[r1]
        self.assertEqual(i1, 'a')

        r2 = cont.register('b')
        i2 = cont[r2]
        i1 = cont[r1]

        self.assertEqual(i2, 'b')
        self.assertEqual(i1, 'a')

    def test_remove(self):
        cont = Container()
        r1 = cont.register('a')
        r2 = cont.remove('a')

        self.assertRaises(KeyError, cont.lookup, 'a')
        self.assertEqual(r1, r2)

        r3 = cont.register('a')
        self.assertNotEqual(r1, r3)

        del cont[r3]
        self.assertRaises(KeyError, cont.lookup, 'a')

    def test_many(self):
        cont = Container()
        steps = [+1, +2, +3, +4, -2, -1, +2, +5, -4, +2, +3, +1, -2, +1, -5]

        active = set()
        ids = set()

        for step in steps:
            if step > 0:
                r = cont.register(step)
                if step in active:
                    self.assertIn(r.id, ids)
                else:
                    self.assertNotIn(r.id, ids)
                    ids.add(r.id)
                    active.add(step)
            else:
                r = cont.remove(-step)
                active.remove(-step)
                ids.remove(r.id)

            self.assertEqual(set(cont.values()), set(active))
            self.assertEqual(set(r.id for r in cont.refs()), set(ids))

    def test_mulit(self):
        a = Container()
        b = Container()

        ra = a.register('a')
        rb = b.register('a')

        self.assertNotEqual(ra, rb)

    def test_container(self):
        cont = Container()
        self.assertEquals(len(cont), 0)

        r1 = cont.register('a')
        self.assertEquals(len(cont), 1)

        r2 = cont.register('b')
        self.assertEquals(len(cont), 2)

        self.assertTrue('a' in cont)
        self.assertTrue('b' in cont)
        self.assertFalse('c' in cont)

        self.assertEqual(set(cont), {'a', 'b'})
        self.assertEqual(cont[r1], 'a')
        self.assertEqual(cont[r2], 'b')

        cont.register('a')
        self.assertEquals(len(cont), 2)

        cont.remove('a')
        self.assertEquals(len(cont), 1)

        cont.register('c')
        self.assertEquals(len(cont), 2)


if __name__ == '__main__':
    unittest.main()
