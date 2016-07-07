class MyStr(object):
    def __init__(self, obj):
        self.obj = obj

    def read(self):
        if isinstance(self.obj, basestring):
            return self.obj
        else:
            return self.obj.read()
