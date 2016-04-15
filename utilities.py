from math import sqrt, radians, sin, cos, asin
from datetime import datetime, timedelta


def haversine(lat1, lon1, lat2, lon2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    #haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # radius of earth in kilometers. use 3956 for miles
    return c * r


def parse_utctime(dt):
    return datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S")


def parse_timezone_offset(offset):
    return timedelta(minutes=int(offset))
