from datetime import datetime, time

def parse_str_to_date(date_str: str, format: str = "%d/%m/%y") -> datetime:
    return datetime.strptime(date_str, format)

def parse_str_to_time(time_str: str, format: str = "%I:%M:%S %p") -> time:
    datetime_object = datetime.strptime(time_str, format)
    return datetime_object.time()