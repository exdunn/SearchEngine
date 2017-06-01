from Ranker import Ranker
from Preloader import Preloader
import sqlite3 as sql
import time
import math
import sys, getopt
argv = sys.argv[1:]
K = 20
PRELOAD = False
PATH = 'Index.db'
BK = 'BookKeep.db'
try:
    opts, args = getopt.getopt(argv, 'hpk:d:b:')
except getopt.GetoptError:
    print 'Arg error'
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print '-p to toggle preloading'
        print '-k <number> to set the top K number'
        print '-d <path> to set the database path'
        print '-b <path> to set the bookkeeping path'
        sys.exit()
    elif opt == '-p':
        PRELOAD = True
    elif opt == '-k':
        K = int(arg)
    elif opt == '-d':
        PATH = arg
    elif opt == '-b':
        BK = arg

p = Preloader(PATH)
r = Ranker(K, PATH)
if PRELOAD:
    p.doc_frequencies()
    p.weights()
while True:
    query = raw_input(':')
    # print type(r)
    # print 'Start Rank Query'
    # start = time.clock()
    links, size, ssize, dur = r.query(query)
    true_size = size if size >= K else ssize

    # print 'AVG GET WEIGHT TIME: ', math.fsum([i - j for i, j in zip(r.ends, r.starts)]) / len(r.starts)

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
        # end = time.clock()
        # print 'TOTAL TIME: ', end - start
        print 'Top', (K if K <= true_size else true_size), 'out of', true_size, '(' + str(size), 'strongly relevant) ' 'relevant documents.'
        print 'Took ~', dur, 's.'
        for l, re in zip(links, res):
            print '{: <15} : {: >5}'.format(l[1], re[0])
            # print str(r[0]).ljust(10), (' : ' + str(l[1])).rjust(10)
    else:
        print 'Top', (K if K <= true_size else true_size), 'out of', true_size, '(' + str(size), 'strongly relevant) ' 'relevant documents.'
        print 'Took ~', dur, 's.'
        print 'No matches found.'

