from pyadds.meta import ops
from pyadds.cooptypes import Co
from pyadds.annotate import cached

from zeroflo import context
from zeroflo.model.base import Combines
from zeroflo.model.links import Links, BuildLinks
from zeroflo.model.spaces import Spaces, BuildSpaces


class Model(Links, Spaces):
    pass


class Builds(ops.operate(Co[Combines, BuildLinks, BuildSpaces])):
    @cached
    def model(self):
        return context.Model()


context.Model = Model
