'''
-Open Bookkeeping.json B:
-Get user input I
-Check user input
-Tokenize user input into tokens
-Parse the tokens into a query
-Create a SELECT query into the DB
    -Get all DocIDs that match
    -For every DocID:
        -Calculate the line position Folder * 500 + File + 1
        -Get that specific line from B
        -Check the DocID (first part) for a match with the DocID in B
        -Load the link into a list
-Print the list
'''
import sqlite3 as sql
import linecache
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import *
DB_PATH = 'C:/Users/Joe/Desktop/Spring2017/CS 121/SearchEngine/Index.db'
BK_PATH = 'C:/Users/Joe/Desktop/Spring2017/CS 121/SearchEngine/WEBPAGES_CLEAN/bookkeeping.json'


def tokenize(string):
    if not string:
        return None
    # print 'string: ', string.encode('utf-8')
    stop = stopwords.words('english')
    return [t for t in
            word_tokenize(string.decode('utf-8'), 'english') if t not in stop]

bk = dict()
with open(BK_PATH) as book:
    for l in book:
        line = l
        if line.find('{') != -1 or line.find('}') != -1:
            continue
        # print line
        line = line.split(':')
        # print line
        line = [l.strip('\n ,') for l in line]
        line = [l[1:-1] for l in line]
        # print line
        bk[line[0]] = line[1]
while True:
    print 'Only accepts one word queries'
    query = raw_input('Query: ')
    query = word_tokenize(query, 'english')
    stemmer = PorterStemmer()
    query = stemmer.stem(query[0])
    doc_ids = list()
    links = list()
    with sql.connect(DB_PATH) as db:
        cursor = db.cursor()
        q = 'SELECT Folder, File FROM Terms WHERE Term == \'' + query + '\''
        cursor.execute(q)
        doc_ids = cursor.fetchall()

    for d in doc_ids:
        id = str(d[0]) + '/' + str(d[1])
        if id in bk:
            links.append(bk[id])
        else:
            print id, ' not found'
        # line_num = d[0] * 500 + d[1]
        # print line_num
        # line = linecache.getline(BK_PATH, line_num)
        # idx = line.find(':')
        # last = line.rfind('"')
        # if last != -1 and last == len(line) - 1:
        #     links.append(line[idx + 3:-2])
        # else:
        #     links.append(line[idx + 3:-1])

    for i, l in enumerate(links):
        print i, ': ', l


# For the report
# Number of Documents Processed: SELECT COUNT(Folder, File) FROM Terms
    # SELECT COUNT(*) FROM (SELECT Folder, File FROM Terms)
# Number of UNIQUE Terms: SELECT DISTINCT COUNT(Term) FROM Terms
    # SELECT COUNT(*) FROM (SELECT DISTINCT Term FROM Terms)
# Number of Strong UNIQUE Terms: SELECT DISTINCT COUNT(Term) FROM Terms WHERE StrongFrequency > 0
    # SELECT COUNTER(*) FROM (SELECT DISTINCT Term FROM Terms WHERE StrongFrequency > 0)
# Most Frequent Term: SELECT Term, MAX(TotalFrequency) FROM (SELECT Term, COUNT(Frequency) AS TotalFrequency FROM Terms GROUP BY Term)
# Size: Check size in explorer