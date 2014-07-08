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

from .util import withctx


class UnitSugar:
    @withctx
    def __and__(self, other, ctx):
        """ `fl1 & fl2` -> `ctx.top.unify(fl1, fl2)` """
        if hasattr(other, '__fl__'):
            ctx.top.unify(self.__fl__, other.__fl__)
            return Union(ctx, self.__fl__)
        else:
            return NotImplemented

    @withctx
    def __or__(self, other, ctx):
        """ `fl1 | fl2` -> `ctx.distribute(fl1, fl2)` """
        if hasattr(other, '__fl__'):
            ctx.top.distribute(self.__fl__, other.__fl__)
            return Distr(ctx, self.__fl__)
        else:
            return NotImplemented

    @withctx
    def __pow__(self, num, ctx):
        """ replicate the processing unit `num` times """
        ctx.top.replicate({self.__fl__}, num)
        return self

    def __rshift__(self, other):
        try:
            port = self.out
        except AttributeError as e:
            return NotImplemented
        return port >> other

    def __rrshift__(self, other):
        try:
            port = self.ins
        except AttributeError as e:
            return NotImplemented
        return other >> port


class OutSugar:
    def __rshift__(self, target):
        """ outport >> inport """
        try:
            return self.unit.ctx.top.link(self, target)
        except NotImplementedError:
            return NotImplemented

    @coroutine
    def __rrshift__(self, packet):
        """ `packet >> outport` -> `outport.handle(packet)` """
        yield from self.handle(packet)


def braceop(f):
    @wraps(f)
    def locy(self, other):
        if hasattr(other, '__fl__'):
            other = Brace(self.ctx, other.__fl__)
        if not isinstance(other, Brace):
            return NotImplemented
        return f(self, other)
    return locy

class Brace:
    def __init__(self, ctx, left, right=None, brace=None):
        self.ctx = ctx
        self.left = left
        self.right = right or left
        self.brace = brace or {self.left, self.right}

    @braceop
    def __ror__(self, other):
        return other.__or__(self)

    @braceop
    def __or__(self, other):
        self.ctx.distribute(self.right, other.left)
        return Distr(self.ctx, self.left, other.right, self.brace.union(other.brace))

    def __pow__(self, num):
        self.ctx.top.replicate(self.brace, num)
        return self

    def __str__(self):
        return '({})'.format(self.__show__.join(map(str, self.fls)))

    def __repr__(self):
        return '{}-of({})'.format(self.__kind__, 
                                  ','.join(map(repr, self.fls)))

class Union(Brace):
    __kind__ = 'union'
    __show__ = '&'
    @braceop
    def __rand__(self, other):
        return other.__and__(self)

    @braceop
    def __and__(self, other):
        self.ctx.unify(self.right, other.left)
        return Union(self.ctx, self.left, other.right, self.brace.union(other.brace))

class Distr(Brace):
    __kind__ = 'distribute'
    __show__ = '|'


