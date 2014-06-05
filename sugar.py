"""
Flow Syntax Sugar
-----------------

Setup Syntax:
 - `unit1 & unit2` -> `ctx.unify(unit1, unit2)`
 - `unit1 | unit2` -> `ctx.distribute(unit1, unit2)`
 - `outport >> inport` -> `ctx.link(outport, inport)`

Flow Syntax:
 - `data >> tag` -> `Packet(data, tag)`
 - `packet >> outport` -> `outport.handle(packet)`
"""

from .tools import unit_ctx

class UnitSugar:
    @unit_ctx
    def __and__(self, other, ctx):
        """ `fl1 & fl2` -> `ctx.unify(fl1, fl2)` """
        if hasattr(other, '__fl__'):
            ctx.unifiy(self.__fl__, other.__fl__)
            return Union(ctx, self.__fl__, other.__fl__)
        else:
            return NotImplemented

    @unit_ctx
    def __or__(self, other, ctx):
        """ `fl1 | fl2` -> `ctx.distribute(fl1, fl2)` """
        if hasattr(other, '__fl__'):
            ctx.distribute(self.__fl__, other.__fl__)
            return Distr(ctx, self.__fl__, other.__fl__)
        else:
            return NotImplemented

class OutSugar:
    def __rshift__(self, target):
        """ `data >> tag` -> `Packet(data, tag)` """
        self.ctx.link(self, target)

    def __rrshift__(self, packet):
        """ `packet >> outport` -> `outport.handle(packet)` """
        self.handle(packet)


class Loc:
    def __init__(self, ctx, *fls):
        self.ctx = ctx
        self.fls = fls

    def all_fls(self, other):
        if hasattr(other, '__fl__'):
            fls = (other,)
        elif isinstance(other, Loc):
            fls = other.fls
        else:
            raise NotImplementedError
        return self.fls + fls

    def __ror__(self, other):
        return self | other

    def __or__(self, other):
        try:
            fls = self.all_fls(other)
        except NotImplementedError:
            return NotImplemented

        self.ctx.distribute(*fls)
        return Distr(self.ctx, *fls)

    def __str__(self):
        return '({})'.format(self.__show__.join(map(str, self.fls)))

    def __repr__(self):
        return '{}-of({})'.format(self.__kind__, 
                                  ','.join(map(repr, self.fls)))

class Union(Loc):
    __kind__ = 'union'
    __show__ = '&'
    def __rand__(self, other):
        return self & other

    def __and__(self, other):
        try:
            fls = self.all_fls(other)
        except NotImplementedError:
            return NotImplemented

        self.ctx.unify(*fls)
        return Union(self.ctx, *fls)

class Distr(Loc):
    __kind__ = 'distribute'
    __show__ = '|'


