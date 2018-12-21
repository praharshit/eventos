from datetime import datetime
from elasticsearch import Elasticsearch
from service import mongo_dao
import uuid
from html.parser import HTMLParser
from bs4 import BeautifulSoup

###
#
# authors: Praharshit, Akshay
#
###

class EsDao(object):
    def __init__(self, index_name):
        self.index_name = index_name
        self.es = Elasticsearch()
        if not self.es.indices.exists(index=index_name):
            self.create_index()

    def create_index(self):
        self.es.indices.create(index=self.index_name)

    def index_es_bulk(self, cursor, index_name, batch_size):
        self.es.indices.delete(index=index_name, ignore=[400, 404])
        self.create_index()
        bulk_data = []
        for n, doc in enumerate(cursor):
            unique_id = uuid.uuid1()
            bulk_data.append({
                "index": {
                    "_index": index_name,
                    "_type": "_doc",
                    "_id": unique_id
                }
            })
            doc.pop('_id', None)
            # try:
            #     doc['text'] = (doc.get('name') or "")+" "+(doc.get('description') or "")+" "+doc.get('category', {}).get('name', "") \
            #         +" "+doc.get('meta_category', {}).get('name', "")
            # except:
            #     pass

            html_parser = HTMLParser()
            processed_html = BeautifulSoup(html_parser.unescape((doc.get('description') or "")), "lxml")
            doc['description'] = processed_html.getText(separator=u' ')
            bulk_data.append(doc)

        start_size = 0
        if len(bulk_data) > batch_size:
            idx = start_size
            while idx < len(bulk_data):
                end = idx + batch_size
                val = len(bulk_data) - idx
                if val < batch_size:
                    end = idx + val
                self.es.bulk(index=index_name, body=bulk_data[idx:end], refresh=True)
                idx += end - idx

        elif len(bulk_data) > 0:
            self.es.bulk(index=index_name, body=bulk_data, refresh=True)


class EsIndex:

    @staticmethod
    def index_mongo_data():
        cursor = mongo_dao.get_data({}, "events_v1")
        es_dao = EsDao("test_v1")
        es_dao.index_es_bulk(cursor=cursor, index_name="test_v1", batch_size=500)


class EsSearch:

    def __init__(self, index_name):
        self.index_name = index_name
        self.es = Elasticsearch()

    def search(self, query, lat, lon):
        if(lat is None or lon is None):
            search_query = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["name^3", "description^2", "category.name^1", "meta_category.name^1"]
                    }
                },
                "size": 50
            }
        else:
            search_query = {
                "sort": [
                    {
                        "_geo_distance": {
                            "venue.location": [lon,lat],
                            "order": "asc",
                            "unit": "km"
                        }
                    }
                ],
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["name^3", "description^2", "category.name^1", "meta_category.name^1"]
                    }
                },
                "size": 50
            }
        response = self.es.search(index=self.index_name, doc_type="_doc", body=search_query)

        return [e['_source'] for e in response['hits']['hits']]