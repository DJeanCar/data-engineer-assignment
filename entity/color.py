class Color:
    def __init__(self, name) -> None:
        self.name = name
        
    def to_dict(self):
        return self.__dict__