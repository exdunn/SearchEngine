# Imports
import os  # For folder, file loop
import re  # For regex checking of strings
import string
from bs4 import BeautifulSoup  # For HTML parsing
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import *
import nltk
from nltk.corpus import stopwords
import sqlite3 as sql

# module for checking file type
import magic

# nltk.download()
# Optimizations
# Lemmatization for Tokens
# Ignore Stop Words, Symbols, and Numbers
# Separates Term - Have Tersm, ID Table, and ID, Doc, Freq Table (Save a bit of space)
DEBUG_QUERY = 0
DEBUG_RES = 0
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
    #     return t
    t = re.search('<title>(.+?)</title>', s.encode('utf-8'))
    if t:
        return t.group(1)

    bi = s.encode('utf-8').find('<body>')
    if bi != -1:
        return s.encode('utf-8')[:bi]
    return None


def get_headers(s):
    hs = list()
    for h in s.find_all('h1'):
        hs.append(h.encode('utf-8'))
    for h in s.find_all('h2'):
        hs.append(h.encode('utf-8'))
    for h in s.find_all('h3'):
        hs.append(h.encode('utf-8'))
    return hs


def get_bold(s):
    bs = list()
    for b in s.find_all('b'):
        bs.append(b.encode('utf-8'))
    for b in s.find_all('strong'):
        bs.append(b.encode('utf-8'))
    return bs


def tokenize(string):
    if not string:
        return None
    # print 'string: ', string.encode('utf-8')
    stop = stopwords.words('english')
    return [t for t in
            word_tokenize(string.decode('utf-8'), 'english') if t not in stop]


def stem(tokens):
    if not tokens:
        return None
    stemmer = PorterStemmer()
    # There is an error here.
    # one of the tokens in  when stemming has an index out of range error
    # when check if the word ends in double consonant
    # why? I have no idea
    # The full error:
    '''
  File "InvertedIndex.py", line 240, in <module>
    title = get_title(soup)
  File "InvertedIndex.py", line 85, in stem
    return [stemmer.stem(t) for t in tokens]
  File "C:\ProgramData\Anaconda2\lib\site-packages\nltk\stem\porter.py", line 665, in stem
    stem = self._step1b(stem)
  File "C:\ProgramData\Anaconda2\lib\site-packages\nltk\stem\porter.py", line 376, in _step1b
    lambda stem: (self._measure(stem) == 1 and
  File "C:\ProgramData\Anaconda2\lib\site-packages\nltk\stem\porter.py", line 258, in _apply_rule_list
    if suffix == '*d' and self._ends_double_consonant(word):
  File "C:\ProgramData\Anaconda2\lib\site-packages\nltk\stem\porter.py", line 214, in _ends_double_consonant
    word[-1] == word[-2] and
IndexError: string index out of range
    '''
    return [stemmer.stem(t) for t in tokens]


def clean_up(tokens):
    if not tokens or len(tokens) == 0:
        return list()
    cleaner = lambda x: x.strip().replace('\'', '')
    return [cleaner(t) for t in tokens]


def remove_unwanted(tokens):
    new_tokens = list()
    for t in tokens:
        if not check_token(t):
            new_tokens.append(t)
    return new_tokens


def check_token(token):
    if re.search('[^\w]', token):
        return True
    if re.search('^[\d.+\-]+$', token):
        return True
    return False

# check for certain file types and return true if they are found
def check_file_type(file_type):
    if file_type.find("data") >= 0:
        print "data"
        return True
    if file_type.find("source") >= 0:
        print "source"
        return True
    if file_type.find("python") >= 0:
        print "python"
        return True
    if file_type.find("C++") >= 0:
        print "c++"
        return True

    return False

    # Check if non ASCII by:
    '''
    try:
        token.decode('ascii')
    except UnicodeDecodeError:
        //Not ascii, so return True
    '''
    # Ignore dates and times
    # Ignore numbers (with k and m, etc.)
    # Ignore single letters non capitalized letters
    # Ignore ordinals like 1st, 2nd, 3rd, ...
            # if not re.search('[^\w]', t) and not re.search('^[\w\d\.+\-=?!@#$%^&*(),.\{}\[]|]+$', t) and not re.search('^[\.\*\+].*$', t) and not re.search('^\d+\-[a-z]+\-\d+$', t) and not re.search('^\d+:\d+:*\d*[a-z]{1,2}$', t):

def add_regular(tokens, doc_id, connector):
    if not tokens or len(tokens) == 0:
        return
    cursor = connector.cursor()
    br = doc_id.find('/')

    for t in tokens:
        # Check if the term, doc_id triple exists in the DB
        query = 'SELECT * FROM Terms WHERE Term == \'' + t.encode('utf-8') \
                + '\' AND Folder == ' + doc_id[:br] \
                + ' AND File == ' + doc_id[br + 1:]
        if DEBUG_QUERY: print query
        cursor.execute(query)

        if not cursor.fetchall():
            # Insert new entry
            query = 'INSERT INTO Terms VALUES(\'' \
                    + t.encode('utf-8') + '\','\
                    + doc_id[:br] + ','\
                    + doc_id[br + 1:] + ','\
                    + '1' + ','\
                    + '0' + ')'
            if DEBUG_QUERY: print query
            cursor.execute(query)
        else:
            # Update new entry
            query = 'SELECT Frequency FROM Terms WHERE Term == \'' \
                    + t.encode('utf-8') + '\' AND Folder == ' + doc_id[:br] \
                    + ' AND File == ' + doc_id[br + 1:]
            if DEBUG_QUERY: print query
            cursor.execute(query)
            freq = cursor.fetchone()[0] + 1
            query = 'UPDATE Terms SET Frequency = ' + str(freq) \
                    + ' WHERE Term == \'' + t.encode('utf-8') \
                    + '\' AND Folder == ' + doc_id[:br] \
                    + ' AND File == ' + doc_id[br + 1:]
            if DEBUG_QUERY: print query
            cursor.execute(query)
    connector.commit()


def add_strong(tokens, doc_id, connector):
    if not tokens or len(tokens) == 0:
        return
    cursor = connector.cursor()
    br = doc_id.find('/')
    for t in tokens:
        # Check if the term, doc_id triple exists in the DB
        query = 'SELECT * FROM Terms WHERE Term == \'' + t.encode('utf-8') \
                + '\' AND Folder == ' + doc_id[:br] \
                + ' AND File == ' + doc_id[br + 1:]
        if DEBUG_QUERY: print query
        cursor.execute(query)

        if not cursor.fetchall():
            # Insert new entry
            query = 'INSERT INTO Terms VALUES(\'' \
                    + t.encode('utf-8') + '\','\
                    + doc_id[:br] + ','\
                    + doc_id[br + 1:] + ','\
                    + '1' + ','\
                    + '1' + ')'
            if DEBUG_QUERY: print query
            cursor.execute(query)
        else:
            # Update new entry
            query = 'SELECT Frequency, StrongFrequency FROM Terms WHERE Term == \'' \
                    + t.encode('utf-8') + '\' AND Folder == ' + doc_id[:br] \
                    + ' AND File == ' + doc_id[br + 1:]
            if DEBUG_QUERY: print query
            cursor.execute(query)
            freqs = cursor.fetchone()
            freq = freqs[0] + 1
            strfreq = freqs[1] + 1
            query = 'UPDATE Terms SET Frequency = ' + str(freq) \
                    + ', StrongFrequency = ' + str(strfreq) \
                    + ' WHERE Term == \'' + t.encode('utf-8') \
                    + '\' AND Folder == ' + doc_id[:br] \
                    + ' AND File == ' + doc_id[br + 1:]
            if DEBUG_QUERY: print query
            cursor.execute(query)
    connector.commit()

# init magic
file_magic = magic.Magic(magic_file="C:/Program Files (x86)/magic/magic.mgc")

# Path to all of the files
PATH = 'WEBPAGES_CLEAN/'
DB_PATH = 'Index.db'
# nltk.download()
# For every file in every folder
docs = 0
counter = 1
for root, folder, files in os.walk(PATH):
    # For every file in one folder
    for fi in files:
        # Check the file type
        if fi.rfind('.json') != -1 or fi.rfind('.tsv') != -1:
            # Ignore the .json and the .tsv file
            continue

        # Get the full path of the file
        full_path = root + '/' + fi
        #print full_path
        file_type = file_magic.from_file(full_path)

        if not check_file_type(file_type):
            pass
        else:
            continue

        # Get the Doc ID
        docid = get_docid(full_path)
        print '#', counter, '; Doc ID: ', docid
        counter += 1
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
            if not title_tokens:
                title_tokens = list()
            body_tokens = stem(tokenize(body))
            if not body_tokens:
                body_tokens = list()
            header_tokens = stem(tokenize(headers))
            if not header_tokens:
                header_tokens = list()
            bold_tokens = stem(tokenize(bolds))
            if not bold_tokens:
                bold_tokens = list()

            all_tokens = clean_up(remove_unwanted(title_tokens + body_tokens))
            strong_tokens = clean_up(remove_unwanted(title_tokens + header_tokens + bold_tokens))

            if DEBUG_RES:
                print 'All Tokens: ', [t for t in all_tokens]
                print 'Strong Tokens Only: ', [s for s in strong_tokens]
                print '################'
            # Add to DB Index
            with sql.connect(DB_PATH) as db:
                # db.text_factory = lambda x: str(x, 'utf-8')
                # db.text_factory = str
                add_regular(all_tokens, docid, db)
                add_strong(strong_tokens, docid, db)
        docs += 1
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


