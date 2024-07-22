class AnimalSighling:
    def __init__(self, park_id, animal_name) -> None:
        self.park_id = park_id
        self.animal_name = animal_name
        
    def to_dict(self):
        return self.__dict__