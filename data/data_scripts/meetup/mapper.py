import math
import bson
import time

from pymongo import MongoClient


###
#
# authors: Akshay
#
###

# mongoclient to run mongod instance
client = MongoClient('mongodb://localhost:27017/')

# connect to db
db = client['meetup']
eventos_db = client['eventos']

common_events = eventos_db['events']


# collection :: table
groups = db['groups']
events = db['events']


results = events.find()

print(results.count())
print(db.events.count_documents({})+1)
for i in range(1, db.events.count_documents({})+1):
    event = results.next()
    print("---------", i)
    mapped_event = {}
    mapped_event.update({"name": event.get("name")})
    mapped_event.update({"description": event.get("description")})
    mapped_event.update({"link": event.get("link")})
    mapped_event.update({"created": event.get("created")})
    mapped_event.update({"status": event.get("status")})
    mapped_event.update({"start_time": event.get("time")})
    start_time = event.get("time")
    duration = event.get("duration")
    if duration is None:
        duration = 0
        end_time = 0
    else:
        end_time = start_time + duration

    mapped_event.update({"end_time": end_time})
    mapped_event.update({"duration": duration})

    venue = {}
    try:
        venue.update({"id": event["venue"]["id"]})
        venue.update({"name": event["venue"]["name"]})
        venue.update({"lat": event["venue"]["lat"]})
        venue.update({"lon": event["venue"]["lon"]})
        venue.update({"address_1": event["venue"]["address_1"]})
        venue.update({"city": event["venue"]["city"]})
        venue.update({"country": event["venue"]["country"]})
        venue.update({"localized_country_name": event["venue"]["localized_country_name"]})
        venue.update({"zip": event["venue"]["zip"]})
        venue.update({"state": event["venue"]["state"]})
        mapped_event.update({"venue": venue})
    except KeyError:
        pass
    mapped_event.update({"price": 0})
    mapped_event.update({"is_free": True})
    group_results = groups.find_one({"id": event["group"]["id"]})
    category = {}
    category.update({"id": group_results["category"]["id"]})
    category.update({"name": group_results["category"]["name"]})
    mapped_event.update({"category": category})
    try:
        meta_category = {}
        meta_category.update({"id": group_results["meta_category"]["id"]})
        meta_category.update({"name": group_results["meta_category"]["name"]})
        mapped_event.update({"meta_category": meta_category})
    except KeyError:
        pass
    common_events.insert_one(mapped_event)

