import sqlite3 as sql
import math

class Preloader:
    STRONG_RELEVANCE = 0.3
    CHAMPIONS = 150

    def __init__(self, path):
        self.path = path
        self.docs = self.__get_doc_number()

    def doc_frequencies(self):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT DISTINCT Term FROM Terms'
            cursor.execute(query)
            terms = cursor.fetchall()
            for t in terms:
                t = str(t[0]) if type(t[0]) is int else t[0].encode('utf-8')
                query = 'SELECT COUNT(*) FROM (SELECT DISTINCT Folder, File FROM Terms WHERE Term == \'' + t + '\')'
                cursor.execute(query)
                df = cursor.fetchone()
                df = df[0] if df else 0

                query = 'SELECT Term FROM DocFrequencies WHERE Term == \'' + t + '\''
                cursor.execute(query)
                if cursor.fetchone():
                    continue

                query = 'INSERT INTO DocFrequencies VALUES (\'' + t + '\', ' + str(df) + ')'
                cursor.execute(query)
            db.commit()

    def weights(self):
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
                if str(a[1]) + '/' + str(a[2]) == cur:
                    wgt = self.__calculate_tf_idf_weight(a[3], a[4], a[5], self.docs)
                    doc_list[a[0]] = wgt
                # Next file AKA Current file is done
                else:
                    length = self.__calculate_query_length(doc_list)
                    for t, w in doc_list.iteritems():
                        t = str(t) if type(t) is int else t.encode('utf-8')
                        nwgt = w / length if length > 0 else 0
                        query = 'UPDATE Terms SET NormWeight == ' + str(nwgt) + ' WHERE Term == \'' + t + '\' AND Folder == ' + str(cur_a[1]) + ' AND File == ' + str(cur_a[2])
                        cursor.execute(query)
                    doc_list.clear()
                    cur = str(cur_a[1]) + '/' + str(cur_a[2])
                    cur_a = a
                    wgt = self.__calculate_tf_idf_weight(a[3], a[4], a[5], self.docs)
                    doc_list[a[0]] = wgt
            db.commit()

    def __get_doc_number(self):
        with sql.connect(self.path) as db:
            cursor = db.cursor()
            query = 'SELECT COUNT(*) FROM (SELECT DISTINCT Folder, File FROM Terms)'
            cursor.execute(query)
            res = cursor.fetchone()
        return res[0] if res else 0

    def __calculate_tf_idf_weight(self, tf, sf, df, n):
        true_tf = tf + (self.STRONG_RELEVANCE * (float(sf) / tf))
        return 1.0 + (math.log10(true_tf) * math.log10(n / (1.0 + df)))

    def __calculate_query_length(self, scores):
        sum = 0
        for _, s in scores.iteritems():
            sum += math.pow(float(s), 2.0)
        return math.sqrt(sum)