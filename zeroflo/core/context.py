from contextlib import contextmanager

_ctx = None

def get_current_context():
    if _ctx is None:
        raise ValueError("No current context, "
                "use the `flo.context(...)` context manaanger before creating "
                "flow units or pass a ctx to the unit constructor.")
    return _ctx

@contextmanager
def context(ctx=None, *args, **kws):
    from .flow import Context
    global _ctx

    old = _ctx
    if not isinstance(ctx, Context):
        ctx = Context(name=ctx, *args, **kws)
    _ctx = ctx
    try:
        yield ctx
    finally:
        _ctx = old
