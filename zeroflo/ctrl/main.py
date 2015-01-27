import asyncio


class Process:
    def __init__(self):
        self.units = {}

    def register(self, uid, unit):
        self.units[uid] = unit

    def open_incoming(self, link):
        pass

    def open_outgoing(self, link):
        pass


class Control:
    def __init__(self, model):
        self.model = model
        self.procs = {}

    def run(self, port, *args, **kws):
        @asyncio.coroutine
        def doit():
            yield from self.setup(port.unit)
            yield from port(*args, **kws)

        asyncio.get_event_loop().run_until_complete(doit())


    @asyncio.coroutine
    def setup(self, unit):
        space = self.model.spaces(unit)
        proc = self.procs[space] = Process()

        proc.register(id(unit), unit)

        for link in self.model.links(targets=unit):
            proc.open_incoming(link)

        for link in self.model.links(sources=unit):
            proc.open_outgoing(link)
