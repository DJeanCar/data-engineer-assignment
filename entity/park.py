class Park:
    def __init__(
        self,
        area_id,
        park_id,
        park_name,
        date,
        start_time,
        end_time,
        total_time,
        park_conditions,
        other_animal_sighlings,
        litter,
        temperature_weather,
    ) -> None:
        self.area_id = area_id
        self.park_id = park_id
        self.park_name = park_name
        self.date = date
        self.start_time = start_time
        self.end_time = end_time
        self.total_time = total_time
        self.park_conditions = park_conditions
        self.other_animal_sighlings = other_animal_sighlings
        self.litter = litter
        self.temperature_weather = temperature_weather

    def to_dict(self):
        return self.__dict__


class ParkBuilder:
    def __init__(self) -> None:
        self.area_id = None
        self.park_id = None
        self.park_name = None
        self.date = None
        self.start_time = None
        self.end_time = None
        self.total_time = None
        self.park_conditions = None
        self.other_animal_sighlings = None
        self.litter = None
        self.temperature_weather = None

    def set_park_details(self, area_id, park_id, park_name, ):
        self.area_id = area_id
        self.park_id = park_id
        self.park_name = park_name
        return self

    def set_date_times(self, date, start_time, end_time, total_time):
        self.date = date
        self.start_time = start_time
        self.end_time = end_time
        self.total_time = total_time
        return self

    def set_aditional_info(
        self, park_conditions, other_animal_sighlings, litter, temperature_weather
    ):
        self.park_conditions = park_conditions
        self.other_animal_sighlings = other_animal_sighlings
        self.litter = litter
        self.temperature_weather = temperature_weather
        return self

    def build(self):
        return Park(
            self.area_id,
            self.park_id,
            self.park_name,
            self.date,
            self.start_time,
            self.end_time,
            self.total_time,
            self.park_conditions,
            self.other_animal_sighlings,
            self.litter,
            self.temperature_weather,
        )
