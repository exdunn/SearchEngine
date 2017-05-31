import math
import sqlite3 as sql
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import *
from nltk.corpus import stopwords


class Preloader:
    # For every term, folder, file in primary key of Terms:
    #   Calculate the TF-IDF Weight
    #   Insert into Scores
    # For every folder, file in Terms:
    #   Calculate the Length (based on Weights in Scores of that folder, file
    #   Insert into Lengths
    # For every term, folder, file in primary key of Terms:
    #   Normalize the weight by dividng by the folder, file's Length
    #   Update the entry
    def __init__(self, path):
        self.path = path

    def preload(self):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT Terms.Term, Folder, File, Frequency, StrongFrequency, DocFrequency FROM Terms, ' \
                    'DocFrequency WHERE Terms.Term == DocFrequency.Term'
            cursor.execute(query)
            all = cursor.fetchall()
            for a in all:



class Ranker:
    STRONG_RELEVANCE = 0.6

    def __init__(self, k, path):
        self.path = path
        self.k = k
        self.docs = self.__get_doc_number()

    def query(self, q):
        tokens = self.__tokenize_query(q)
        query_wgts = self.__calculate_query_weights(tokens)  # Calculates the weights for the query
        relevant_docs = self.__get_relevant_docs(tokens) # Gets a list of docs which have at least one token in
        cs_k = None
        # common with the query
        for d in relevant_docs:
            wgts = self.__get_weights(d) # Gets the weights(a column) for that doc
            cs = self.__get_cos_score(query_wgts, wgts)  # Calculates the dot product of q and d (For every t in q,
            # search for the t in d and multiply their weights. If not found, then 0. Finally, sum all products)
            cs_k = self.__select_k(cs)  # Checks if this cs is good enough for now to be in the top k

        return cs_k

    def __get_doc_number(self):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT COUNT(*) FROM (SELECT DISTINCT Folder, File FROM Terms)'
            cursor.execute(query)
            res = cursor.fetchone()
            docs = res[0] if res else 0
        return docs

    def __tokenize_query(self, q):
        stop = stopwords.words('english')
        tokens = [t for t in word_tokenize(q.decode('utf-8'), 'english') if t not in stop]
        stemmer = PorterStemmer()
        return [stemmer.stem(t) for t in tokens]

    def __calculate_query_weights(self, tokens):
        scores = dict()
        for t in tokens:
            if t in scores:
                continue
            tf = tokens.count(t)
            df = self.__get_doc_freq(t)
            wgt = self.__calculate_tf_idf_weight(tf, 0, df, self.docs)
            scores[t] = wgt

        length = self.__calculate_query_length(scores)
        for t, _ in scores.iteritems():
            scores[t] = self.__normalize_weight(scores[t], length)

        return scores

    def __get_doc_freq(self, term):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT DocumentFrequency FROM DocFreqs WHERE Term == \'' + term.encode('utf-8') + '\''
            cursor.execute(query)
            res = cursor.fetchone()
            if not res:
                print token.encode('utf-8'), ' does not exist in DocFreqs.'
            df = res[0] if res else 0
        return df

    def __calculate_tf_idf_weight(self, tf, sf, df, n):

    def __calculate_query_length(self, scores):

    def __normalize_weight(self, wgt, length):
        return wgt / length if length > 0 else 0