import time
import requests
import math
from pymongo import MongoClient

###
#
# authors: Akshay
#
###


meetup_domain_url = "https://api.meetup.com/"
find_groups = "find/groups"
find_events = "/events"

print("fetching response...")
print("Response is:")

# mongoclient to run mongod instance
client = MongoClient('mongodb://localhost:27017/')

# connect to db
db = client['meetup']

# collection :: table
groups = db['groups1']

params = {"sign": "true", "key": "683e12191db6e691d687a6ea737443"}
response = requests.get(meetup_domain_url + find_groups, params)

total_groups_count = response.headers.get("x-total-count")
max_page_size = 200
total_pages = int(total_groups_count) / max_page_size + 1
print(response.headers)
print(total_groups_count)
total_pages = int(math.floor(total_pages))
print(total_pages)

for counter in range(1, total_pages):
    params = {"sign": "true", "key": "683e12191db6e691d687a6ea737443", "page": max_page_size, "offset": counter}
    response = requests.get(meetup_domain_url + find_groups, params)
    time.sleep(1)
    print(counter)
    groups.insert_many(response.json())
    time.sleep(1)
print("done!")
