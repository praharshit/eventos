"""Query Processing

Process query

"""
import re
import csv
import math
import time
from collections import OrderedDict
import service.mongo_dao
from service.index import Tokenizer, LinguisticModules, InvertedIndex


class Search:
    def __init__(self):
        self.search_indexes = None
        self.get_search_indexes()

    @staticmethod
    def calc_documents_list(input_index):
        """
        Get document ids available in the index
        :param input_index:
        :return: set of docids
        """
        doc_ids = set()
        [doc_ids.update(posting_list.keys()) for posting_list in input_index[1]]
        return doc_ids

    def score_query_per_document(self, input_index, query, doc_id):
        """
        Scoring function for a query in a document
        :param input_index:
        :param query:
        :param doc_id:
        :return: score
        """
        dictionary = input_index[0]
        posting_lists = input_index[1]
        terms = self.process_query(query)
        n = len(self.calc_documents_list(input_index))
        score = 0
        for term in terms:
            try:
                term_posting_list = posting_lists[dictionary[term][1]]
            except:
                term_posting_list = {}
            term_doc_ids = list(term_posting_list.keys())
            if doc_id in term_doc_ids:
                score += (1 + math.log(int(term_posting_list[doc_id]), 2)) * math.log(n / int(dictionary[term][0]))
        return score


    def get_search_indexes(self):
        indexes = list(service.mongo_dao.get_data({}, 'search_indexes'))
        indexes = dict((index['index_type'], (index['dictionary'], index['posting_lists'])) for index in indexes)
        self.search_indexes = indexes

    @staticmethod
    def process_query(query):
        """
        Process simple text queries of the form Term1 Term2 Term3

        :param query:
        :return: list of terms
        """
        terms = Tokenizer().tokenize(query)
        # Normalize tokens
        normalized_terms = list(map(lambda term: LinguisticModules().normalize(term), terms))
        # Stem tokens
        stemmed_terms = list(map(lambda term: LinguisticModules().stem(term), normalized_terms))
        # Remove stopwords
        cleaned_terms = list(filter(lambda term: term not in LinguisticModules().stopwords, stemmed_terms))
        return cleaned_terms


    @staticmethod
    def process_word(word):
        """
        Process a single word
        :param word:
        :return: processed word
        """
        # Normalize word
        normalized_word = LinguisticModules().normalize(word)
        # Stem word
        stemmed_word = LinguisticModules().stem(word)
        # Remove if stopword
        cleaned_word = "" if stemmed_word in LinguisticModules.stopwords else stemmed_word
        return cleaned_word

    def search_in_index(self, query, search_index):
        docs = Search.calc_documents_list(search_index)
        query_scores = list(filter(
            lambda s: s[1] != 0, [(doc, self.score_query_per_document(search_index, query, doc))
                                  for doc in docs]))
        return query_scores

    def search(self, query):
        if self.search_indexes is None:
            return []
        else:
            name_index = self.search_indexes['name']
            desc_index = self.search_indexes['desc']
            name_scores = self.search_in_index(query, name_index)
            # name_scores = sorted(sorted(name_scores, key=lambda tup: (int(tup[0]))), key=lambda tup: (tup[1]), reverse=True)
            desc_scores = self.search_in_index(query, desc_index)
            # desc_scores = sorted(sorted(desc_scores, key=lambda tup: (int(tup[0]))), key=lambda tup: (tup[1]), reverse=True)
            print(name_scores, desc_scores)
            return []




# if __name__ == "__main__":
#     index_filepath = input("Enter index filepath(E.g. output.csv if available in the same directory or fully qualified path): ")
#     index = parse_index(index_filepath)
#     print("Index loaded")
#     input_queries = input("Enter comma seperated queries(E.g. query1, query2): ")
#     results_output_filepath = input("Enter query results output filepath(E.g. results.txt): ")
#     input_queries = [Query(query) for query in input_queries.split(",")]
#     query_scores_per_document = {}
#     for query in input_queries:
#         docs_filtered = calc_documents_list(index)
#         query_scores_per_document[query.original_query] = \
#             list(filter(lambda s: s[1] != 0, [(doc, score_query_per_document(index, query.raw_query, doc))
#                                               for doc in docs_filtered]))
#     # Write query results
#     with open(results_output_filepath, 'a') as out_results:
#         time.ctime()
#         out_results.write("-----------------------------------------------------------------------------------------\n")
#         out_results.write("Results - "+time.strftime('%l:%M%p %Z on %b %d, %Y')+"\n")
#         for query, scores in query_scores_per_document.items():
#             out_results.write("\nQuery: "+query+"\n")
#             scores = sorted(sorted(scores, key=lambda tup: (int(tup[0]))), key=lambda tup: (tup[1]), reverse=True)
#             if scores:
#                 [out_results.write("DOC "+score[0]+" - "+str(score[1])+"\n") for score in scores]
#             else:
#                 out_results.write("No document matches found\n")
#
#     # Print query results
#     print("Query ranked results")
#     for query, scores in query_scores_per_document.items():
#         print("")
#         print("Query -", query)
#         if not scores: print("No document matches found")
#         [print("DOC",score[0],"-",str(score[1]))
#          for score in sorted(sorted(scores, key=lambda tup: (int(tup[0]))), key=lambda tup: (tup[1]), reverse=True)]