import os
from bs4 import BeautifulSoup

path = 'C:/Users/Joe/Desktop/Spring2017/CS 121/BeautifulSoupTest/WEBPAGES_CLEAN/'
for root, dirs, files in os.walk(path):
    for f in files:
        fp = root + '/' + f
        print fp

        # Ignore the json
        if fp.rfind('.json') != -1:
            continue
        if fp.rfind('.tsv') != -1:
            continue


        # Get Doc ID
        foid = -1; fiid = -1
        first_break = fp.rfind('/')
        if first_break != -1:
            fiid = fp[first_break + 1:]
        second_break = fp[:first_break].rfind('/')
        if second_break != -1:
            foid = fp[second_break + 1:first_break]
        # End
        doc_id = str(foid) + '/' + str(fiid)
        # Or alternatively
        br = fp.rfind('/')
        if br != -1:
            br = fp[:br].rfind('/')
            if br != -1:
                doc_id = fp[br + 1:]
        print "Doc ID: " + doc_id

        with open(fp) as inp:
            soup = BeautifulSoup(inp, 'html.parser')
            print soup
            soup.prettify()
            # print soup.title.text if soup.title else '-'
            # Start Find Title
            title = soup.title.text if soup.title else ''
            body_index = str(soup).find('<body>')
            print 'Body_Index: ' + str(body_index)
            if body_index != -1:
                title = str(soup)[:body_index]
            # End Find Title
            print 'Title: ' + title
            # print 'Only Text: ' + soup.get_text()
            headers = list()
            # Get Header Tokens
            for h in soup.find_all('h1'):
                print 'H1: ' + h
                headers.append(h)
            for h in soup.find_all('h2'):
                print 'H2: ' + h
                headers.append(h)
            for h in soup.find_all('h3'):
                print 'H3: ' + h
                headers.append(h)
            # End
            strong = list()
            strong_check = len(strong)
            # Get Bold/Strong Tokens
            for b in soup.find_all('b'):
                print 'B: ' + b
                strong.append(b)
            for b in soup.find_all('strong'):
                print 'strong: ' + b
                strong.append(b)
            # End
            if strong_check < len(strong):
                raw_input('New strongs')
                strong_check = len(strong)

            # Load to DB

            raw_input('Done')