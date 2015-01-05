class IdGen:
    def __init__(self):
        self.next_id = 0

    def next(self):
        id = self.next_id
        self.next_id += 1
        return id

    def free(self, id):
        pass


class ReusingGen(IdGen):
    def __init__(self):
        super().__init__()
        self.free_ids = []

    def next(self):
        if self.free_ids:
            return self.free_ids.pop(0)
        else:
            return super().next()

    def free(self, id):
        self.free_ids.append(id)


class Container:
    def __init__(self):
        self._by_id = {}
        self._by_item = {}

        self._ids = IdGen()

    def add(self, item):
        try:
            id = self._by_item[item]
        except KeyError:
            id = self._ids.next()
            self._by_id[id] = item
            self._by_item[item] = id
        return id

    def remove(self, item):
        id = self._by_item[item]
        self._ids.free(id)
        return id
