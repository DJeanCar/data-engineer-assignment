class FurColor:
    def __init__(self, park_id, squirrel_id, primary_fur_color, highlights_in_fur_color, color_notes) -> None:
        self.park_id = park_id
        self.squirrel_id = squirrel_id
        self.primary_fur_color = primary_fur_color
        self.highlights_in_fur_color = highlights_in_fur_color
        self.color_notes = color_notes
        
    def to_dict(self):
        return self.__dict__
