__author__ = 'dwae'

import unittest
from zeroflo.top.refer import Container

class ContainerTestCase(unittest.TestCase):
    def test_unique(self):
        """
        equal items should give same refs
        """
        cont = Container()

        r1 = cont.register('a')
        r2 = cont.register('a')
        self.assertEqual(r1, r2)

        cont.register('b')
        cont.register('c')
        r3 = cont.register('a')
        self.assertEqual(r1, r3)

    def test_differ(self):
        """
        different items should give back different refs
        """
        cont = Container()

        r1 = cont.register('a')
        r2 = cont.register('b')
        self.assertNotEqual(r1, r2)

        cont.register('a')
        r3 = cont.register('c')
        self.assertNotEqual(r1, r3)
        self.assertNotEqual(r2, r3)

    def test_lookup(self):
        """
        one should be able to lookup refs again
        """
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
        """
        one should be able to remove objects
        """
        cont = Container()
        r1 = cont.register('a')
        r2 = cont.remove('a')

        self.assertRaises(KeyError, cont.lookup, 'a')
        self.assertEqual(r1, r2)

        r3 = cont.register('a')
        self.assertIsNot(r1, r3)

        del cont[r3]
        self.assertRaises(KeyError, cont.lookup, 'a')

    def test_container(self):
        """
        container should have container behaviour
        """
        cont = Container()
        self.assertEquals(len(cont), 0)

        r1 = cont.register('a')
        self.assertEquals(len(cont), 1)

        r2 = cont.register('b')
        self.assertEquals(len(cont), 2)

        self.assertTrue('a' in cont)
        self.assertTrue('b' in cont)
        self.assertFalse('c' in cont)

        self.assertEqual(set(cont), {'a','b'})
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
