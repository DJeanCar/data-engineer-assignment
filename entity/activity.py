class Activity:
    def __init__(self, squirrel_id, activity_name) -> None:
        self.squirrel_id = squirrel_id
        self.activity_name = activity_name
        
    def to_dict(self):
        return self.__dict__