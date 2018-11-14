import json
import os


def get_stop_words():
    stop_words_path = "../../resources/stopwords.json"
    with open(os.path.join(os.path.dirname(__file__), stop_words_path)) as sw_json:
        stopwords_data = json.load(sw_json)
        stopwords = []
        if 'stopWords' in stopwords_data and stopwords_data['stopWords'] is not None:
            stopwords.extend(stopwords_data['stopWords'])

        # TO-DO add from other stopwords sources

        # Combining and removing duplicate stopwords
        stopwords = list(set(stopwords))
        return stopwords