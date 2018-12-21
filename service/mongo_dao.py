from pymongo import MongoClient

###
#
# authors: Praharshit
#
###

conn = MongoClient("localhost", 27017)
db = conn["eventos"]


def insert_query(data_json, collection):
    return db[collection].insert_one(data_json)


def get_data(query, collection, limit=None):
    if limit:
        return db[collection].find(query).limit(limit)
    else:
        return db[collection].find(query)


def update_query(query_json, update_json, collection):
    return db[collection].update(query_json, update_json, upsert=True)




