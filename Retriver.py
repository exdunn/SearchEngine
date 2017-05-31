from Ranker import Ranker
import sqlite3 as sql
import time
import math
K = 10
PATH = 'Index.db'
BK = 'BookKeep.db'
r = Ranker(K, PATH)
# r.do_doc_freq()
# r.preload2()
while True:
    query = raw_input(':')
    # print type(r)
    # print 'Start Rank Query'
    start = time.clock()
    links = r.query(query)

    print 'AVG GET WEIGHT TIME: ', math.fsum([i - j for i, j in zip(r.ends, r.starts)]) / len(r.starts)

    # print 'End Rank Query'
    res = list()
    # print links
    if len(links) > 0:
        with sql.connect(BK) as db:
            cursor = db.cursor()
            query = 'SELECT Link FROM Links WHERE (Folder == ' + str(links[0][0][0]) + ' AND File == ' + str(links[0][0][
                                                                                                                 1])\
                    + ')'
            for l in links[1:]:
                query += ' OR (Folder == ' + str(l[0][0]) + ' AND File == ' + str(l[0][1]) + ')'
            # print query
            cursor.execute(query)
            res = cursor.fetchall()
        end = time.clock()
        print 'TOTAL TIME: ', end - start
        for l, re in zip(links, res):
            print '{: <15} : {: >5}'.format(l[1], re[0])
            # print str(r[0]).ljust(10), (' : ' + str(l[1])).rjust(10)
    else:
        print 'No matches found.'

