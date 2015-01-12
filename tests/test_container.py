import pytest

from zeroflo.top import container
from collections import namedtuple


class TestGen:
    def do_it(self, gen, a=4, b=2, c=4):
        ids = set()
        seen = set()
        for i in range(a):
            id = gen.next()
            assert id not in ids
            ids.add(id)
            seen.add(id)

        for i in range(b):
            id = ids.pop()
            gen.free(id)

        for i in range(c):
            id = gen.next()
            assert id not in ids
            ids.add(id)
            seen.add(id)
        return seen

    def test_simple_gen(self):
        seen = self.do_it(container.IdGen())
        assert len(seen) == 8

    def test_reusing_gen(self):
        seen = self.do_it(container.ReusingGen())
        assert len(seen) == 6


class TestContainer:
    elems = ['a', 'b', 'c', 'd', 'e']
    Cont = container.Container

    def setup(self):
        self.a, self.b, self.c, self.d, self.e = self.elems

    def test_unique(self):
        cont = self.Cont()

        r1 = cont.add(self.a)
        r2 = cont.add(self.a)
        assert r1 == r2

        cont.add(self.b)
        cont.add(self.c)
        r3 = cont.add(self.a)
        assert r1 == r3

    def test_strs(self):
        cont = self.Cont('test')

        r = cont.add(self.a)
        assert "test" in str(r)
        assert str(r.id) in str(r)

        cont.add(self.b)
        assert str(self.a) in str(cont)
        assert str(self.b) in str(cont)

    def test_differ(self):
        cont = self.Cont()
        r1 = cont.add(self.a)
        r2 = cont.add(self.b)
        assert r1 != r2

        cont.add(self.a)
        r3 = cont.add(self.c)
        assert r1 != r3
        assert r2 != r3

    def test_lookup(self):
        cont = self.Cont()

        r1 = cont.add(self.a)
        i1 = cont[r1]
        assert i1 == self.a

        r2 = cont.add(self.b)
        i2 = cont[r2]
        i1 = cont[r1]

        assert i2 == self.b
        assert i1 == self.a

    def test_remove(self):
        cont = self.Cont()
        r1 = cont.add(self.a)
        r2 = cont.remove(self.a)

        with pytest.raises(KeyError):
            cont.index(self.a)
        assert r1 == r2

        r3 = cont.add(self.a)
        assert r1 == r3

        del cont[r3]
        with pytest.raises(KeyError):
            cont.index(self.a)

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
                    assert r in ids
                else:
                    assert r not in ids
                    ids.add(r)
                    active.add(elem)
            else:
                elem = self.elems[-step-1]
                r = cont.remove(elem)
                active.remove(elem)
                ids.remove(r)

            assert set(cont.values()) == set(active)
            assert set(cont.keys()) == set(ids)

    def test_mulit(self):
        a = self.Cont()
        b = self.Cont()

        ra = a.add(self.a)
        rb = b.add(self.a)

        assert ra != rb
        assert not ra == rb

    def test_container(self):
        cont = self.Cont()
        assert len(cont) == 0

        r1 = cont.add(self.a)
        assert len(cont) == 1

        r2 = cont.add(self.b)
        assert len(cont) == 2

        assert self.a in cont
        assert self.b in cont
        assert self.c not in cont

        assert set(cont) == {self.a, self.b}
        assert cont[r1] == self.a
        assert cont[r2] == self.b

        cont.add(self.a)
        assert len(cont) == 2

        cont.remove(self.a)
        assert len(cont) == 1

        cont.add(self.c)
        assert len(cont) == 2

    def test_setitem(self):
        cont = self.Cont()
        r1 = cont.add(self.a)
        r2 = cont.add(self.b)

        cont[4] = self.c

        assert self.c in cont
        assert cont[4] == self.c
        assert cont.index(self.c).id == 4

        cont[r1.id] = self.a
        assert r1 == cont.index(self.a)

        with pytest.raises(IndexError):
            # set other value to same id
            cont[r2.id] = self.d

        with pytest.raises(IndexError):
            # set same value to other id
            cont[5] = self.b


class TestReferedContainer(TestContainer):
    Cont = container.ReferedContainer

    def test_container(self):
        cont = self.Cont()

        a = cont.add(self.a, 0)
        b = cont.add(self.b, 0)

        cont[3:1] = self.c
        c = cont.index(self.c)

        assert set(cont.refered_values(0)) == {self.a, self.b}
        assert set(cont.refered_values(1)) == {self.c}

        assert set(cont.refered_keys(0)) == {a, b}
        assert set(cont.refered_keys(1)) == {c}

        assert set(cont.refered_items(0)) == {(a, self.a), (b, self.b)}
        assert set(cont.refered_items(1)) == {(c, self.c)}

        cont.dismiss(0)

        assert set(cont) == {self.c}


Cons = namedtuple('Cons', 'car, cdr')


class TestAssocContainer:
    Cont = container.AssocContainer
    elems = [Cons('a', 'b'), Cons('b', 'c'), Cons('d', 'e'),
             Cons('e', 'f'), Cons('f', 'g')]

    def test_assoc(self):
        cont = self.Cont()

        ab = cont.add(Cons('a', 'b'))
        ac = cont.add(Cons('a', 'c'))
        dc = cont.add(Cons('d', 'c'))
        da = cont.add(Cons('d', 'a'))

        assert set(cont.refered_keys(car='a')) == {ab, ac}
        assert set(cont.refered_keys(cdr='a')) == {da}
        assert set(cont.refered_keys(cdr='c')) == {ac, dc}
        assert set(cont.refered_keys(car='a', cdr='c')) == {ac}

        cont.dismiss(car='a')

        assert set(cont) == {Cons('d', 'c'), Cons('d', 'a')}


class TestSetsContainer(TestContainer):
    Cont = container.SetsContainer
    elems = [('a',), ('b',), ('f',), ('c',), ('d',)]

    def test_sets(self):
        cont = self.Cont()

        ab = cont.add({'a', 'b'})
        bc = cont.add({'b', 'c'})
        cont.add({'d', 'e'})

        assert set(cont.refered_keys('b')) == {ab, bc}
        assert next(iter(cont.refered_values('e'))) == {'d', 'e'}


class TestGroups(TestContainer):
    Cont = container.GroupedContainer

    def test_groups(self):
        cont = self.Cont()

        cont.join('a', 'b', 'c')
        cont.join('d', 'e')
        cont.part('e', 'f')
        cont.part('c', 'e')
        cont.join('f', 'g')
        cont.part('h', 'b')
        cont.part('d', 'g')

        assert len(cont) == 4
        assert {'a', 'b', 'c'} in cont
        assert {'d', 'e'} in cont
        assert {'f', 'g'} in cont
        assert {'h'} in cont

        with pytest.raises(IndexError):
            cont.join('a', 'e')

        with pytest.raises(IndexError):
            cont.part('a', 'c')
