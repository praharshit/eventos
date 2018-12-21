"""Inverted index

Implementation to construct inverted index

"""

import re
import csv
from nltk.stem import PorterStemmer
from collections import OrderedDict
from service.util import utils
import service.mongo_dao
from bson import ObjectId

###
#
# authors: Praharshit, Chirag
#
###

class LinguisticModules:
    """Linguistic Modules for token processing"""

    stemmer=PorterStemmer()
    stopwords=utils.get_stop_words()

    def normalize(self, text):
        """
        Normalize text to lower case, remove special characters
        :param text:
        :return: text
        """
        regex_sub_pattern="['-]"
        if type(text)==list:
            text=[t.lower() for t in text]
            text=[re.sub(regex_sub_pattern, "", t) for t in text]
        else:
            text=text.lower()
            text = re.sub(regex_sub_pattern, "", text)
        return text

    def stem(self, text):
        """
        Stem text using NLTK PorterStemmer
        :param text:
        :return: text
        """
        if type(text)==list:
            text=[self.stemmer.stem(t) for t in text]
        else:
            text=self.stemmer.stem(text)
        return text

    def remove_stopwords(self, words):
        """
        Remove pre-defined set of stopwords.txt from the input words
        :param words:
        :return: words
        """
        return list(set(words) - set(self.stopwords))


class Tokenizer:
    """Tokenizer to split text based on pre-defined patterns"""

    tokenizer_regex="[^a-zA-Z0-9-']"

    def tokenize(self, document):
        """
        Split a given text based on pre-defined regex pattern
        :param document:
        :return: tokens
        """
        raw_tokens = re.split(self.tokenizer_regex, document)
        tokens = list(filter(lambda token: token!='', raw_tokens))
        return tokens

class InvertedIndex:
    """Inverted index class"""
    # Instance of linguistic modules
    linguistic_models=LinguisticModules()
    token_sequence=[]
    # Structure of dictionary OrderedDict(Key: token, Value: tuple(doc_frequency, posting_list_index))
    dictionary=OrderedDict()
    # Structure of  posting_list is a simple list
    posting_lists=[]

    def prepare_index(self, documents):
        """
        Creates an inverted index for input text documents
        :param documents: A dictionary with key:document Id, value:raw text
        :return: dictionary, posting_lists
        """
        self.token_sequence=self.process_documents(documents)
        # Sort by tokens and then by docIDs
        self.token_sequence.sort(key=lambda token: (token[0], token[1]))
        # Create Dictionary with doc frequency and positional posting list with term frequencies
        for token_data in self.token_sequence:
            # if token exists in dictionary, increase doc frequency entry in dictionary against the token
            # and update positional posting list
            if token_data[0] in self.dictionary:
                posting_list_index=self.dictionary[token_data[0]][1]
                if self.update_positional_info(token_data[1], token_data[2], posting_list_index):
                    self.dictionary[token_data[0]] = (self.dictionary[token_data[0]][0]+1, posting_list_index)
            # else create a new entry in the dictionary and positional posting list for the token
            else:
                self.posting_lists.append(OrderedDict({token_data[1]: 1}))
                self.dictionary[token_data[0]] = (1, len(self.posting_lists)-1)
        return self.dictionary, self.posting_lists

    def update_positional_info(self, doc_id, position, posting_list_index):
        """
        Update positional posting list
        :param doc_id:
        :param position:
        :param posting_list_index:
        :return: True or False if new list entry is added in the positional posting list
        """
        new_entry = False
        if doc_id in self.posting_lists[posting_list_index]:
            # self.posting_lists[posting_list_index][doc_id] = self.posting_lists[posting_list_index][doc_id][0]+1, self.posting_lists[posting_list_index][doc_id][1]+[position])
            self.posting_lists[posting_list_index][doc_id] = self.posting_lists[posting_list_index][doc_id]+1
        else:
            # self.posting_lists[posting_list_index][doc_id] = (1, [position])
            self.posting_lists[posting_list_index][doc_id] = 1
            new_entry = True
        return new_entry


    def process_documents(self, documents):
        """
        Process text of documents and apply linguistic modules on all extracted tokens
        :param documents: A dictionary with key:document Id, value:raw text
        :return: cleaned_tokens
        """
        # Document wise raw tokens
        raw_tokens=list(map(lambda doc: list(map(lambda item: (item[1], doc[0], item[0]), enumerate(Tokenizer().tokenize(doc[1])))), documents.items()))
        # Merge tokens list across all documents
        original_tokens=[token for doc_tokens in raw_tokens for token in doc_tokens]
        # Normalize tokens
        normalized_tokens=list(map(lambda token: (self.linguistic_models.normalize(token[0]), token[1], token[2]), original_tokens))
        # Stem tokens
        stemmed_tokens = list(map(lambda token: (self.linguistic_models.stem(token[0]), token[1], token[2]), normalized_tokens))
        # Remove stopwords.txt
        cleaned_tokens=list(filter(lambda token: token[0] not in self.linguistic_models.stopwords, stemmed_tokens))
        return cleaned_tokens


def index_data(data, key):
    documents = dict((str(data[0]['_id']), d[key]) for d in data)
    dictionary, posting_lists = InvertedIndex().prepare_index(documents)
    return dictionary, posting_lists

if __name__ == "__main__":
    data = list(service.mongo_dao.get_data({}, collection="events"))
    print(data)
    name_index = index_data(data, 'name')
    service.mongo_dao.update_query({'index_type':'name'}, {'$set':{'dictionary':name_index[0], 'posting_lists': name_index[1]}}, 'search_indexes')
    desc_index = index_data(data, 'description')
    service.mongo_dao.update_query({'index_type':'desc'}, {'$set':{'dictionary':name_index[0], 'posting_lists': name_index[1]}}, 'search_indexes')

