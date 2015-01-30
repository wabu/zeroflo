from pyadds.meta import ops
from pyadds.cooptypes import Co
from pyadds.annotate import delayed

from zeroflo.model.base import Combines
from zeroflo.model.units import Units
from zeroflo.model.links import Links, BuildLinks
from zeroflo.model.spaces import Spaces, BuildSpaces


class Model(Units, Links, Spaces):
    pass


from zeroflo import context


class Builds(ops.operate(Co[Combines, BuildLinks, BuildSpaces])):
    @delayed
    def model(self):
        return context.mk_model()

    @model.post
    def model(self):
        self.model.register(self)