# Imports
import os  # For folder, file loop
import re  # For regex checking of strings
from bs4 import BeautifulSoup  # For HTML parsing
from nltk.tokenize import word_tokenize
from nltk.stem import *
import sqlite3 as sql


def get_docid(path):
    # Find the first path break / (Should be in between the Folder and File)
    br = path.rfind('/')
    if br != -1:
        # Find the second path break (Should be in front of the folder)
        br = path[:br].rfind('/')
        if br != -1:
            # The Doc ID is Folder/File
            return path[br + 1:]
    return None


def get_body(s):
    b = s.body
    return b.text.encode('utf-8') if b else None


def get_title(s):
    # t = s.title.text if soup.title else None
    # if t:
    #     return t.encode('utf-8')
    t = re.search('<title>(.+?)</title>', str(s))
    if t:
        print 'regex match'
        return t.group(1).encode('utf-8')

    bi = str(s).find('<body>')
    if bi != -1:
        print bi
        return str(s)[:bi].encode('utf-8')
    return None


def get_headers(s):
    hs = list()
    for h in s.find_all('h1'):
        hs.append(h)
    for h in s.find_all('h2'):
        hs.append(h)
    for h in s.find_all('h3'):
        hs.append(h)
    return hs


def get_bold(s):
    bs = list()
    for b in s.find_all('b'):
        bs.append(b)
    for b in s.find_all('strong'):
        bs.append(b)
    return bs


def tokenize(string):
    return word_tokenize(string)


def stem(tokens):
    stemmer = PorterStemmer()
    return [stemmer.stem(t) for t in tokens]


def add_regular(tokens, doc_id, connector):
    cursor = connector.cursor()
    br = doc_id.find('/')
    for t in tokens:
        # Check if the term, doc_id triple exists in the DB
        query = 'SELECT * FROM Terms WHERE Terms.Term == ' + str(t) \
                + ' AND Terms.Folder == ' + str(doc_id[:br]) \
                + ' AND Terms.File == ' + str(doc_id[br + 1:])
        cursor.execute(query)

        if not cursor.fetchall():
            # Insert new entry
            query = 'INSERT INTO Terms VALUES(' \
                    + str(t) + ','\
                    + str(doc_id[:br]) + ','\
                    + str(doc_id[br + 1:]) + ','\
                    + str(1) + ','\
                    + str(0) + ')'
            cursor.execute(query)
        else:
            # Update new entry
            query = 'SELECT Frequency FROM Terms WHERE Terms.Term == ' \
                    + str(t) + ' AND Terms.Folder == ' + str(doc_id[:br]) \
                    + ' AND Terms.File == ' + str(doc_id[br + 1:])
            cursor.execute(query)
            freq = cursor.fetchone()[0] + 1
            query = 'UPDATE Terms SET Terms.Frequency = ' + str(freq) \
                    + ' WHERE Terms.Term == ' + str(t) \
                    + ' AND Terms.Folder == ' + str(doc_id[:br]) \
                    + ' AND Terms.File == ' + str(doc_id[br + 1:])
            cursor.execute(query)
    connector.commit()


def add_strong(tokens, doc_id, connector):
    cursor = connector.cursor()
    br = doc_id.find('/')
    for t in tokens:
        # Check if the term, doc_id triple exists in the DB
        query = 'SELECT * FROM Terms WHERE Terms.Term == ' + str(t) \
                + ' AND Terms.Folder == ' + str(doc_id[:br]) \
                + ' AND Terms.File == ' + str(doc_id[br + 1:])
        cursor.execute(query)

        if not cursor.fetchall():
            # Insert new entry
            query = 'INSERT INTO Terms VALUES(' \
                    + str(t) + ','\
                    + str(doc_id[:br]) + ','\
                    + str(doc_id[br + 1:]) + ','\
                    + str(1) + ','\
                    + str(1) + ')'
            cursor.execute(query)
        else:
            # Update new entry
            query = 'SELECT Frequency, StrongFrequency FROM Terms WHERE Terms.Term == ' \
                    + str(t) + ' AND Terms.Folder == ' + str(doc_id[:br]) \
                    + ' AND Terms.File == ' + str(doc_id[br + 1:])
            cursor.execute(query)
            freqs = cursor.fetchone()
            freq = freqs[0] + 1
            strfreq = freqs[1] + 1
            query = 'UPDATE Terms SET Terms.Frequency = ' + str(freq) \
                    + ', Terms.StrongFrequency = ' + str(strfreq) \
                    + ' WHERE Terms.Term == ' + str(t) \
                    + ' AND Terms.Folder == ' + str(doc_id[:br]) \
                    + ' AND Terms.File == ' + str(doc_id[br + 1:])
            cursor.execute(query)
    connector.commit()


# Path to all of the files
PATH = 'C:/Users/Joe/Desktop/Spring2017/CS 121/SearchEngine/WEBPAGES_CLEAN/'
DB_PATH

# For every file in every folder
for root, folder, files in os.walk(PATH):
    # For every file in one folder
    for fi in files:
        # Check the file type
        if fi.rfind('.json') != -1 or fi.rfind('.tsv') != -1:
            # Ignore the .json and the .tsv file
            continue
        # Get the full path of the file
        full_path = root + '/' + fi
        # Get the Doc ID
        docid = get_docid(full_path)
        # print 'Doc ID: ' + docid
        if docid is None:
            print 'Bad Doc ID for path: ', full_path
            continue

        # Open the file and do some parsing
        with open(full_path) as inf:
            # Use BeautifulSoup to parse
            soup = BeautifulSoup(inf, 'html.parser')

            # Get the Body
            body = get_body(soup)
            # print 'Body: ', body
            # Get the Title
            title = get_title(soup)
            # print 'Title: ', title
            # Get the Headers
            headers = get_headers(soup)
            # Get the Bold
            bolds = get_bold(soup)

            # Tokenize via NLTK
            title_tokens = stem(tokenize(title))
            all_tokens = stem(tokenize(body)) + title_tokens
            strong_tokens = title_tokens + stem(tokenize(headers)) + stem(tokenzie(bolds))

            # Add to DB Index
            with sql.connect(DB_PATH) as db:
                add_regular(all_tokens, docid, db)
                add_strong(strong_tokens, docid, db)

            raw_input()
#  Schtuff
# DB Format
'''
Table Terms:
    -Term STRING,
    -Folder INT,
    -File INT,
    -Frequency INT,
    -Strong Frequency INT (This can be split)
    PK (Term, Folder, File)
Table Weights:
    -Term STRING,
    -Folder INT,
    -File INT,
    -TF-IDF Weight REAL
    PK (Term, Folder, File)
TF-IDF Weight = LOG_10(1 + TF) * LOG_10 (N/DF)
TF = f(FREQ, STRONG FREQ)
N = SELECT COUNT(UNIQUE(FOLDER)) * COUNT(UNIQUE(FILE)) FROM Terms (# of Docs total)
DF = SELECT COUNT(*) FROM Terms WHERE Term = T (# of times T appears in a unique Doc)
'''
# Queries
'''
Given a Query Q of a single Term T, Get the DocID
-SELECT Folder, File FROM Terms WHERE Term = T
Given a Query Q of multiple Terms Ti, Get the DocID
-OR Have the WHERE Term = T part be an OR of all Terms
-AND "" AND of all Terms
-Better way, Use TF-IDF Weights and SUM the Weights for each Term T to get a Table of each DocID and the Q TF-IDF Weights. Sort by Weight Descending.
Given a Query Q of a single Term T, Get the Weights for some Doc D
-SELECT TF-IDF Weight FROM Weights WHERE Term = T AND File = F AND Folder = O
Given a Query Q of many Terms T1 ... Ti, find the N most relevant DocIDs
-SELECT Folder, File, SUM(TF-IDF Weight) AS Score FROM Weights WHERE (Term = T1 OR Term = T2 OR ... OR Term = Ti) GROUP BY Folder, File ORDER BY Score DESC ( (Hopefully this gets a table with folder, file and the sum of weights for only the terms in Q)
-
'''
# Indexing
'''
Via os.walk - open every Folder O, File F:
    -Ignore .json and .tsv
    -Calculate the Full Path FP
    -Get the Doc ID O/F
    -Open File:
        -Parse the File via BeautifulSoup
        -Get the body:
            -Tokenize the body into tokens
            -Insert into DB via SQLite (Token, Doc ID)
                -If found, update FREQ
                -If not found, add new entry with FREQ = 1
        -Get the title:
            -Tokenize the title into tokens
            -Insert into DB via SQLite
                -Same as Body, except also update STRONG FREQ as well
        -Get the headers, strongs, and bolds:
            -Tokenize into tokens
            -Insert into DB
                -Same as Title, except do not update FREQ
'''

# Retrieving
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

