class Property:
    def __init__(self, key: str):
        self.key = key

    def __str__(self):
        return self.key


class ArrayProperty(Property):
    def __init__(self, key: str):
        super(ArrayProperty, self).__init__(key)
