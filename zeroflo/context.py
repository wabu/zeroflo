from zeroflo.model import Model

class Context:
    def mk_model(self):
        return Model()

__context__ = Context()

def get_context():
    return __context__

def mk_model():
    return __context__.mk_model()

def set_context(context):
    global __context__
    assert isinstance(context, Context)
    __context__ = context
