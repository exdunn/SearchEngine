
# Open bookkeep database
# open bookkeep file
# Store Folder, File, Link into Database for every entry in file
PATH = 'BookKeep.db'
import sqlite3 as sql
with open('WEBPAGES_CLEAN\\bookkeeping.json') as bk:
    with sql.connect(PATH) as db:
        cursor = db.cursor()
        for line in bk:
            if line.find('{') != -1 or line.find('}') != -1:
                continue
            line = line.split(':')
            line = [l.strip('\n ,')[1:-1] for l in line]
            folder = line[0].split('/')[0]
            file = line[0].split('/')[1]
            link = line[1]

            query = 'SELECT Folder, File FROM Links WHERE Folder == ' + str(folder) + ' AND File == ' + str(file)
            cursor.execute(query)
            if cursor.fetchone():
                continue

            query = 'INSERT INTO Links VALUES (' + str(folder) + ', ' + str(file) + ', \'' + link.encode('utf-8') + \
                    '\')'
            cursor.execute(query)
        db.commit()