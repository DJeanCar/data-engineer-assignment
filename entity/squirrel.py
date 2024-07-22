class Squirrel:
    def __init__(
        self,
        park_id,
        squirrel_id,
        location,
        above_ground,
        specific_location,
        interactions_with_humans,
        observations,
        latitude,
        longitude,
    ) -> None:
        self.park_id = park_id
        self.squirrel_id = squirrel_id
        self.location = location
        self.above_ground = above_ground
        self.specific_location = specific_location
        self.interactions_with_humans = interactions_with_humans
        self.observations = observations
        self.latitude = latitude
        self.longitude = longitude

    def to_dict(self):
        return self.__dict__


class SquirrelBuilder:
    def __init__(self) -> None:
        self.park_id = None
        self.squirrel_id = None
        self.location = None
        self.above_ground = None
        self.specific_location = None
        self.interactions_with_humans = None
        self.observations = None
        self.latitude = None
        self.longitude = None

    def set_squirrel_id(self, park_id, squirrel_id):
        self.park_id = park_id
        self.squirrel_id = squirrel_id
        return self

    def set_location(
        self, location, above_ground, specific_location, latitude, longitude
    ):
        self.location = location
        self.above_ground = above_ground
        self.specific_location = specific_location
        self.latitude = latitude
        self.longitude = longitude
        return self

    def set_aditional_info(self, interactions_with_humans, observations):
        self.interactions_with_humans = interactions_with_humans
        self.observations = observations
        return self

    def build(self):
        return Squirrel(
            self.park_id,
            self.squirrel_id,
            self.location,
            self.above_ground,
            self.specific_location,
            self.interactions_with_humans,
            self.observations,
            self.latitude,
            self.longitude,
        )
