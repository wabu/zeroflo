"""
The combine defines simple ways to combine packets from inports:
>>> class Add:
...     @outport
...     def outs(): pass
...
...     @combine
...     def add(self, a, b, tag):
...         a+b >> tag >> self.outs   
...
...     @async
...     @inport
...     def a(self, a, tag):
...         yield from self.combine(a=a, tag=tag)
...
...     @async
...     @inport
...     def b(self, b, tag):
...         yield from self.combine(b=b, tag=tag)

It includes
- `combine`: combine args from different async inports
- `join(on : str)`: join args on same tag attribute
"""
