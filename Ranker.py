import math
import sqlite3 as sql
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import *
from nltk.corpus import stopwords
from heapq import nsmallest
import time
import itertools
path = 'Index.db'

# Speed up Cos Calculations
# Cluster Pruning: Pick sqrt(N) at random -> leaders
    # Precompute nearest leader for every non-leader
    # Find the nearest leader to Q
    # Get K nearest from the leader's followers
# Champion Lists: For each term t, choose r highest weighted doc's as champions
    # Go through these docs first.
    # If result is less than K, then go through the rest

# Space optimization
# Inverted Index should try to calculate the normalized weights for each Term and store it in Terms
# Remove Scores table
# Still need docfrequencies index tho
class Ranker:
    STRONG_RELEVANCE = 0.3
    IDF_THRESHOLD = 0.01
    QUERY_RELEVANCE = 0.5
    CHAMPIONS = 150

    def __init__(self, k, path):
        self.path = path
        self.k = k
        self.docs = self.__get_doc_number()
        self.cursor = None

    def query(self, q):
        start = time.clock()
        with sql.connect(self.path) as db:
            self.cursor = db.cursor()
            tokens = self.__tokenize_query(q)
            if not tokens or len(tokens) == 0:
                print 'No input (No significant input)'
                return list()
            query_wgts = self.__calculate_query_weights(tokens)  # Calculates the weights for the query
            relevant_docs = self.__get_relevant_docs(tokens) # Gets a list of docs which have at least one token in
            semi_relevant_docs = list()
            if (relevant_docs and len(relevant_docs) < self.k) or not relevant_docs:
                semi_relevant_docs = self.__get_relevant_docs_any(tokens)
            cs_k = dict()
            # common with the query
            docs = relevant_docs if len(relevant_docs) >= self.k else semi_relevant_docs
            for d in docs:
                wgts = self.__get_weights(d[0], d[1]) # Gets the weights(a column) for that doc
                cs = self.__get_cos_score(query_wgts, wgts)  # Calculates the dot product of q and d (For every t in q,
                # search for the t in d and multiply their weights. If not found, then 0. Finally, sum all products)
                cs_k[d] = cs
            cs_k = self.__select_k(cs_k, self.k)
        end = time.clock()
        return cs_k, len(relevant_docs), len(semi_relevant_docs), end - start

    def __get_doc_number(self):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT COUNT(*) FROM (SELECT DISTINCT Folder, File FROM Terms)'
            cursor.execute(query)
            res = cursor.fetchone()
        return res[0] if res else 0

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
            if wgt >= self.IDF_THRESHOLD:
                scores[t] = wgt

        length = self.__calculate_query_length(scores)
        for t, _ in scores.iteritems():
            scores[t] = self.__normalize_weight(scores[t], length)

        return scores

    def __get_relevant_docs(self, tokens):
        # print len(tokens)
        num_matches = max(int(len(tokens) * self.QUERY_RELEVANCE), 1)
        combs = itertools.combinations(tokens, num_matches)
        # print num_matches
        # for c in combs:
            # print c

        query = 'SELECT DISTINCT Folder, File FROM Terms WHERE '
        first = True
        for c in combs:
            if first:
                first = False
                query += '('
            else:
                query += ' OR ('
            inner_first = True
            for t in c:
                if inner_first:
                    inner_first = False
                else:
                    query += ' AND '
                query += 'Term == \'' + t.encode('utf-8') + '\''
            query += ')'
        # print query
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        return res if res else list()

    def __get_relevant_docs_any(self, tokens):
        query = 'SELECT DISTINCT Folder, File FROM Terms WHERE Term == \'' + tokens[0].encode('utf-8') + '\''
        for t in tokens[1:]:
            query += ' OR Term == \'' + t.encode('utf-8') + '\''
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        return res if res else list()

    def __get_weights(self, folder, file):
        # self.starts.append(time.clock())
        query = 'SELECT Term, NormWeight FROM Terms WHERE Folder == ' + str(folder) + ' AND File == ' + str(file)
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        if not res:
            return dict()
        wgts = dict()
        # self.ends.append(time.clock())
        for r in res:
            wgts[r[0]] = r[1]

        return wgts

    def __get_cos_score(self, qwgt, dwgt):
        # return 0
        cs = 0
        for t, s in qwgt.iteritems():
            # print 't: ', t
            # print 'dwgt: ', dwgt
            if t.encode('utf-8') in dwgt:
                # print 't in dwgt'
                cs += s * dwgt[t]
        return cs

    def __select_k(self, cs_k, k):
        if k == -1:
            return cs_k
        return nsmallest(k, cs_k.iteritems(), key=lambda (k, v): (-v, k))

    def __get_doc_freq(self, term):
        query = 'SELECT DocFrequency FROM DocFrequencies WHERE Term == \'' + term.encode('utf-8') + '\''
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        if not res:
            pass
            # print term.encode('utf-8'), ' does not exist in DocFrequencies.'
        return res[0] if res else 0

    def __calculate_tf_idf_weight(self, tf, sf, df, n):
        true_tf = tf + (self.STRONG_RELEVANCE * sf)
        return 1.0 + (math.log10(true_tf) * math.log10(n / (1.0 + df)))

    def __calculate_query_length(self, scores):
        sum = 0
        for _, s in scores.iteritems():
            sum += math.pow(float(s), 2.0)
        return math.sqrt(sum)

    def __normalize_weight(self, wgt, length):
        return wgt / length if length > 0 else 0


