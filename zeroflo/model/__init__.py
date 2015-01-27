from pyadds.meta import ops
from pyadds.cooptypes import Co
from pyadds.annotate import cached

from zeroflo.model.base import Combines
from zeroflo.model.links import Links, BuildLinks
from zeroflo.model.spaces import Spaces, BuildSpaces


class Model(Links, Spaces):
    pass

from zeroflo import context


class Builds(ops.operate(Co[Combines, BuildLinks, BuildSpaces])):
    @cached
    def model(self):
        mdl = context.mk_model()
        mdl.register(self)
        return mdl

