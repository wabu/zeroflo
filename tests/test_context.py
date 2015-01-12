from zeroflo.top import context, topology
import pathlib

import gc


def test_root():
    ctx = context.Context(name='testflo')

    root = ctx.root.name
    assert root.split('/')[-1].startswith('__')
    assert root.endswith('__')
    assert 'testflo' in root
    assert pathlib.Path(root).is_dir()

    del ctx
    gc.collect()
    assert not pathlib.Path(root).exists()


def test_default_ctx():
    @context.with_ctx
    def get_other(ctx):
        return ctx

    ctx = context.get_context()
    assert isinstance(ctx, context.Context)
    assert ctx is context.get_context()
    assert ctx is get_other()

    assert pathlib.Path(ctx.root.name).is_dir()


def test_bind():
    ctx = context.Context('aname')

    other = context.get_context()
    assert other is not ctx
    assert 'aname' not in other.root.name
    assert context.get_context(ctx=ctx) is ctx

    with ctx.bind():
        assert context.get_context() is ctx
        assert 'aname' in ctx.root.name
        assert context.get_context(ctx=ctx) is ctx

    other = context.get_context()
    assert other is not ctx
    assert 'aname' not in other.root.name
    assert context.get_context(ctx=ctx) is ctx


def test_attrs():
    ctx = context.Context()

    assert isinstance(ctx.topology, topology.Topology)
