

class executor:
    def __init__(self, id):
        self.id = id
        self.status = 'Prepared'

    def __delete__(self, instance):
        pass
