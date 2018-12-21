import requests
import time
from pymongo import MongoClient

###
#
# author: Akshay
#
###

meetup_domain_url = "https://api.meetup.com/"
find_groups = "find/groups"
find_events = "/events"

print("fetching response...")
print("Response is:")
# print(response.text)
# print(response.headers)

# mongoclient to run mongod instance
client = MongoClient('mongodb://localhost:27017/')

# connect to db
db = client['meetup']

# collection :: table
groups = db['groups']
events = db['events']
results = groups.find()

for i in range(1, results.count()+1):
    params = {"sign": "true", "key": "683e12191db6e691d687a6ea737443"}
    url = meetup_domain_url + results.next().get('urlname') + find_events
    print(i, url)
    response = requests.get(url, params)
    time.sleep(1)
    #print(response.text)
    if len(response.json()) > 1:
        events.insert_many(response.json())
        time.sleep(1)