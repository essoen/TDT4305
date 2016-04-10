from datetime import datetime, timedelta
from utilities import haversine
from collections import OrderedDict


def record_to_object(o):
    o = o.split('\t')
    return OrderedDict({
        'checkin_id': o[0],
        'user_id': o[1],
        'session_id': o[2],
        'utc_time': o[3],
        'timezone_offset': o[4],
        'lat': o[5],
        'lon': o[6],
        'category': o[7],
        'subcategory': o[8]
    })


def calculate_local_time(o):
    utc_time = datetime.strptime(str(o['utc_time']), "%Y-%m-%d %H:%M:%S")
    timezone_offset = timedelta(minutes=int(o['timezone_offset']))
    o['local_time'] = (utc_time + timezone_offset).strftime("%Y-%m-%d %H:%M:%S")
    return o


def find_nearest_city_and_country(o):
    lat1, lon1 = float(o['lat']), float(o['lon'])

    start = 10000000000.0
    country, city = '', ''
    for c in fsCountries:
        dist = haversine(lat1, lon1, float(c[1]), float(c[2]))
        if dist < start:
            country = c[4]
            city = c[0]
            start = dist

    o['city'] = city
    o['country'] = country
    return o


def calculate_distance(o):
    checkins = sorted(o[1], key=lambda x: datetime.strptime(str(x['local_time']), "%Y-%m-%d %H:%M:%S"))
    distance = 0.0
    for i in range(len(checkins) - 1):
        c_1 = checkins[i]
        c_2 = checkins[i + 1]
        distance += (haversine( float(c_1['lat']), float(c_1['lon']), 
            float(c_2['lat']), float(c_2['lon']) ))

    return (o[0], o[1], distance)
