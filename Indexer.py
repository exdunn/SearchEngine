
import os
import re
from bs4 import BeautifulSoup

import magic

class Indexer(object):

    path = ""

    def __init__(self):
        pass

    def tokenize(self):
        count = 0
        limit = 100000

        path = "C:/Users/alexh/PycharmProjects/SearchEngine/WEBPAGES_CLEAN"

        for root, dirs, file_names in os.walk(self.path):

            for f in file_names:

                data = {}
                full_path = root + '/' + f
                source_path = root[64:] + '/' + f

                data['source_path'] = source_path

                if int(f.find(".json")) != -1 or int(f.find(".tsv")) != -1:
                    print("irrelevant file : ", f)
                    continue

                try:
                    file_type = magic.from_file(full_path, mime=True)
                    data['file_type'] = file_type




