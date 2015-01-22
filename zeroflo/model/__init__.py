from pyadds.meta import ops
from pyadds.annotate import delayed

from zeroflo import context
from zeroflo.model.base import MayCombine
from zeroflo.model.links import Links, MayLink
from zeroflo.model.spaces import Spaces, MaySpace


class Model(Links, Spaces):
    pass


@ops.operate
class _Builds(MayCombine, MayLink, MaySpace):
    pass

class Builds(_Builds):
    @delayed
    def model(self):
        return context.Model()


context.Model = Model
