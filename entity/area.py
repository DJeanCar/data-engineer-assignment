class Area:
    def __init__(self, area_id, area_name) -> None:
        self.area_id = area_id
        self.area_name = area_name
        
    def to_dict(self):
        return self.__dict__

class AreaBuilder:
    def __init__(self) -> None:
        self.area_id = None
        self.area_name = None

    def set_id(self, area_id):
        self.area_id = area_id
        return self

    def set_name(self, area_name):
        self.area_name = area_name
        return self

    def build(self):
        return Area(self.area_id, self.area_name)
