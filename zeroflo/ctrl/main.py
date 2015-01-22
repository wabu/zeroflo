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
    def __init__(self, topology):
        self.topology = topology

    def call(self, port, *args, **kws):
        pass
