import math
import sqlite3 as sql
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import *
from nltk.corpus import stopwords
from heapq import nsmallest
import time
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
    IDF_THRESHOLD = 0.1
    CHAMPIONS = 150

    def __init__(self, k, path):
        self.path = path
        self.k = k
        self.docs = self.__get_doc_number()
        self.cursor = None
        self.starts = list()
        self.ends = list()

    def do_doc_freq(self):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT DISTINCT Term FROM Terms'
            cursor.execute(query)
            terms = cursor.fetchall()
            for t in terms:
                if type(t[0]) is int:
                    t = str(t[0])
                else:
                    t = t[0].encode('utf-8')
                # if t == 'comput':
                    # print 'comput found'
                query = 'SELECT COUNT(*) FROM (SELECT DISTINCT Folder, File FROM Terms WHERE Term == \'' + t + '\')'
                cursor.execute(query)
                df = cursor.fetchone()
                df = df[0] if df else 0

                query = 'SELECT Term FROM DocFrequencies WHERE Term == \'' + t + '\''
                cursor.execute(query)
                if cursor.fetchone():
                    # if t == 'comput':
                    #     print 'comput found again'
                    continue

                query = 'INSERT INTO DocFrequencies VALUES (\'' + t + '\', ' + str(df) + ')'
                cursor.execute(query)
            db.commit()

    def preload(self):
        doc_lengths = dict()
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT Terms.Term, Folder, File, Frequency, StrongFrequency, DocFrequency FROM Terms, ' \
                    'DocFrequency WHERE Terms.Term == DocFrequency.Term'
            cursor.execute(query)
            all = cursor.fetchall()
            for a in all:
                wgt = self.__calculate_tf_idf_weight(a[3], a[4], a[5], self.docs)
                query = 'INSERT INTO Scores VALUES (\'' + a[0].encode('utf-8') + '\', ' + str(folder) + ', ' + str(file) \
                        + ', ' + str(wgt) + ', 0)'
                cursor.execute(query)

            query = 'SELECT DISTINCT Folder, File FROM Terms'
            cursor.execute(query)
            docs = cursor.fetchall()
            for d in docs:
                query = 'SELECT Weight FROM Scores WHERE Folder == ' + str(d[0]) + ' AND File == ' + str(d[1])
                cursor.execute(query)
                wgts = cursor.fetchone()
                length = self.__calculate_query_length(wgts)
                id = str(d[0]) + '/' + str(d[1])
                doc_lengths[id] = length
                # query = 'INSERT INTO Lengths VALUES (' + str(d[0]) + ', ' + str(d[1]) + ', ' + str(length) + ')'
                # cursor.execute(query)

            query = 'SELECT Terms.Term, Terms.Folder, Terms.File, Weights FROM Terms, Scores WHERE Terms.Term == ' \
                    'Scores.Term AND Terms.Folder == Scores.Folder AND Terms.File == Scores.File'
            cursor.execute(query)
            wgts = cursor.fetchall()
            for w in wgts:
                n_wgt = self.__normalize_weight(w[3], doc_lengths[str(w[0] + '/' + str(w[1]))])
                query = 'UPDATE Scores SET NormWeight = ' + str(n_wgt) + ' WHERE Term == \'' + w[0].encode('utf-8') +\
                        '\' AND Folder == ' + str(w[1]) + ' File == ' + str(w[2])
                cursor.execute(query)

            db.commit()

    def preload2(self):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT Terms.Term, Folder, File, Frequency, StrongFrequency, DocFrequency FROM Terms, ' \
                    'DocFrequencies WHERE Terms.Term == DocFrequencies.Term ORDER BY Folder, File'
            cursor.execute(query)
            all = cursor.fetchall()
            cur = str(all[0][1]) + '/' + str(all[0][2])
            cur_a = all[0]
            doc_list = dict()
            for a in all:
                # Same file as current
                # print cur
                # print a[1], '/', a[2]
                if str(a[1]) + '/' + str(a[2]) == cur:
                    wgt = self.__calculate_tf_idf_weight(a[3], a[4], a[5], self.docs)
                    doc_list[a[0]] = wgt
                # Next file AKA Current file is done
                else:
                    length = self.__calculate_query_length(doc_list)
                    for t, w in doc_list.iteritems():
                        if type(t) is int:
                            t = str(t)
                        else:
                            t = t.encode('utf-8')
                        query = 'SELECT Term FROM Scores WHERE Term == \'' + t + '\'' + ' AND Folder ' \
                                                                                                        '== ' + str(
                                cur_a[1]) + ' AND File == ' + str(cur_a[2])
                        cursor.execute(query)
                        if cursor.fetchone():
                            continue

                        nwgt = w / length if length > 0 else 0
                        query = 'INSERT INTO Scores VALUES (\'' + t + '\', ' + \
                                str(cur_a[1]) + ', ' + str(cur_a[2]) + ', ' + str(nwgt) + ')'
                        cursor.execute(query)
                    doc_list.clear()
                    cur = str(cur_a[1]) + '/' + str(cur_a[2])
                    cur_a = a
                    wgt = self.__calculate_tf_idf_weight(a[3], a[4], a[5], self.docs)
                    doc_list[a[0]] = wgt
            db.commit()

    def query(self, q):
        with sql.connect(self.path) as db:
            self.cursor = db.cursor()
            # print 'Start Tokenize'
            tokens = self.__tokenize_query(q)
            # print 'End Tokenize'
            if not tokens or len(tokens) == 0:
                print 'No input (No significant input)'
                return list()
            # print 'Start Query Weights'
            query_wgts = self.__calculate_query_weights(tokens)  # Calculates the weights for the query
            # print 'End Query Weights'
            # print 'Start Relevant Docs'
            relevant_docs = self.__get_relevant_docs(tokens) # Gets a list of docs which have at least one token in
            # print 'End Relevant Docs'
            cs_k = dict()
            # common with the query
            # print relevant_docs
            print 'SIZE OF RELEVANT DOCS: ', len(relevant_docs)
            print '######################Relevant Doc Loop'
            for d in relevant_docs:
                # print 'Start Weights'
                wgts = self.__get_weights(d[0], d[1]) # Gets the weights(a column) for that doc
                # print 'End Weights'
                # print 'Start CS'
                cs = self.__get_cos_score(query_wgts, wgts)  # Calculates the dot product of q and d (For every t in q,
                # print 'End CS'
                # search for the t in d and multiply their weights. If not found, then 0. Finally, sum all products)
                cs_k[d] = cs
            print '####################End Relevant Doc Loop'
            # print 'Start Select'
            cs_k = self.__select_k(cs_k, self.k)
            # print 'End Select'

        return cs_k

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
        query = 'SELECT DISTINCT Folder, File FROM Terms WHERE Term == \'' + tokens[0].encode('utf-8') + '\''
        for t in tokens[1:]:
            query += ' OR Term == \'' + t.encode('utf-8') + '\''
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        return res if res else list()

    def __get_weights(self, folder, file):
        self.starts.append(time.clock())
        query = 'SELECT Term, NormWeight FROM Scores WHERE Folder == ' + str(folder) + ' AND File == ' + str(file)
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        if not res:
            return dict()
        wgts = dict()
        self.ends.append(time.clock())
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
            print term.encode('utf-8'), ' does not exist in DocFrequencies.'
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



# r = Ranker(10, path)
# # r.do_doc_freq()
# # r.preload2()
# query = raw_input(':')
# print r.query(query)