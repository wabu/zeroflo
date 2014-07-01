
class Managed(Idd):
    instances = ref.WeakKeyDictionary()

    def __new__(cls, *args, **kws):
        self = super().__init__(cls, *args, **kws)
        cls.instances[self.id] = None



