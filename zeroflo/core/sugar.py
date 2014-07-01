"""
Flow Syntax Sugar
-----------------

Setup Syntax:
 - `unit1 & unit2` -> `ctx.top.unify(unit1, unit2)`
 - `unit1 | unit2` -> `ctx.top.distribute(unit1, unit2)`
 - `outport >> inport` -> `ctx.link(outport, inport)`

Flow Syntax:
 - `data >> tag` -> `Packet(data, tag)`
 - `packet >> outport` -> `outport.handle(packet)`
"""

from asyncio import coroutine
from functools import wraps

from .util import ctxmethod

class UnitSugar:
    @ctxmethod
    def __and__(self, other, ctx):
        """ `fl1 & fl2` -> `ctx.top.unify(fl1, fl2)` """
        if hasattr(other, '__unit__'):
            ctx.top.unify(self.__unit__, other.__unit__)
            return Union(ctx, self.__unit__, other.__unit__)
        else:
            return NotImplemented

    @ctxmethod
    def __or__(self, other, ctx):
        """ `fl1 | fl2` -> `ctx.distribute(fl1, fl2)` """
        if hasattr(other, '__unit__'):
            ctx.top.distribute(self.__unit__, other.__unit__)
            return Distr(ctx, self.__unit__, other.__unit__)
        else:
            return NotImplemented

class OutSugar:
    def __rshift__(self, target):
        """ `data >> tag` -> `Packet(data, tag)` """
        self.unit.ctx.top.link(self, target)

    @coroutine
    def __rrshift__(self, packet):
        """ `packet >> outport` -> `outport.handle(packet)` """
        yield from self.handle(packet)


def locop(f):
    @wraps(f)
    def locy(self, other):
        if hasattr(other, '__unit__'):
            other = Loc(self.ctx, other.__unit__)
        if not isinstance(other, Loc):
            return NotImplemented
        return f(self, other)
    return locy

class Loc:
    def __init__(self, ctx, *fls):
        self.ctx = ctx
        self.fls = fls

    @property
    def left(self):
        return self.fls[0]

    @property
    def right(self):
        return self.fls[-1]

    @locop
    def __ror__(self, other):
        return self.dist(other.right, self.left)

    @locop
    def __or__(self, other):
        return self.dist(self.right, other.left)

    def dist(self, *fls):
        self.ctx.top.distribute(*fls)
        return Distr(self.ctx, *fls)

    def __str__(self):
        return '({})'.format(self.__show__.join(map(str, self.fls)))

    def __repr__(self):
        return '{}-of({})'.format(self.__kind__, 
                                  ','.join(map(repr, self.fls)))

class Union(Loc):
    __kind__ = 'union'
    __show__ = '&'
    @locop
    def __rand__(self, other):
        return self.union(other.right, self.left)

    @locop
    def __and__(self, other):
        return self.union(self.right, other.left)

    def union(self, *fls):
        self.ctx.top.unify(*fls)
        return Union(self.ctx, *fls)

class Distr(Loc):
    __kind__ = 'distribute'
    __show__ = '|'


