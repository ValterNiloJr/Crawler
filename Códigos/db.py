from urllib.parse import urljoin
from bs4 import BeautifulSoup
import urllib3
import pymysql
import re
import nltk
#nltk.download()


class Crawler():
    def __init__(self, pages, depth=1):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        for i in range(depth):
            new_pages = set()
            for page in pages:
                http = urllib3.PoolManager()
                try:
                    page_data = http.request('GET', page)
                except:
                    print('Open page ERROR: ' + page )
                    continue
                
                soap = BeautifulSoup(page_data.data, "lxml")
                self.indexer(page, soap)
                links = soap.find_all('a')
                count = 1
                
                for link in links:
                    if ('href' in link.attrs):
                        url = urljoin(page, str(link.get('href')))
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]
                        if url[:4] == 'http':
                            new_pages.add(url)
                        count += 1
                pages = new_pages

    def insertWordLocation(self, idurl, idword, location):
        # for insert data into db, use autocommit
        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db', autocommit=True)
        cursor = connection.cursor()

        cursor.execute('insert into word_location (idurl, idword, location) values (%s, %s, %s)', (idurl, idword, location))
        id_word_location = cursor.lastrowid

        cursor.close()
        connection.close()

        return id_word_location


    def insertWord(self, word):
        # for insert data into db, use autocommit
        # use unicode
        # charset utf8mb4, same of db
        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db', autocommit=True, use_unicode=True, charset='utf8mb4')
        cursor = connection.cursor()

        cursor.execute('insert into words (word) values (%s)', word)
        
        id_word = cursor.lastrowid # only localhost

        cursor.close()
        connection.close()

        return id_word


    def indexedWord(self, word):
        _return = -1 # Word not exists
        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db', use_unicode=True, charset='utf8mb4')
        cursor = connection.cursor()

        cursor.execute('select idword from words where word = %s', word)

        if cursor.rowcount > 0:
            _return = cursor.fetchone()[0]
        
        #else:
            # word not exists, return = -1

        cursor.close()
        connection.close()

        return _return


    def insertPage(self, url):
        # for insert data into db, use autocommit
        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db', autocommit=True)
        cursor = connection.cursor()

        cursor.execute('insert into urls (url) values (%s)', url)

        id_page = cursor.lastrowid # only localhost

        cursor.close()
        connection.close()

        return id_page


    def indexedPage(self, url):
        _return = -1 # Page not exists

        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db')
        cursor_url = connection.cursor()

        cursor_url.execute('select idurl from urls where url = %s', url)

        if cursor_url.rowcount > 0:
            #print('url cadastrada')
            idurl = cursor_url.fetchone()[0]
            cursor_word = connection.cursor()
            cursor_word.execute('select idurl from word_location where idurl = %s', idurl)

            if cursor_word.rowcount > 0:
                _return = -2 # Page exists with registered words

            else:
                _return = idurl # Page exists without registered words, so return Page ID
                
            cursor_word.close()

        #else:
            # Page not exists, return = -1

        cursor_url.close()
        connection.close()

        return _return


    def separateWords(self, text):
        stop = nltk.corpus.stopwords.words('portuguese')
        stemmer = nltk.stem.RSLPStemmer() # RSLPStemmer() -> stemmer for Brazil
        splitter = re.compile('\\W+')
        word_list = []
        aux_list = [word for word in splitter.split(text) if word != '']
        for word in aux_list:
            if word.lower() not in stop and len(word) > 1:
                word_list.append(stemmer.stem(word).lower())
        
        return word_list


    def getText(self, soap):
        for tags in soap(['script', 'style']):
            tags.decompose()

        return ' '.join(soap.stripped_strings)


    def indexer(self, url, soap):
        indexed = self.indexedPage(url)
        
        if indexed == -2: # Page exists with registered words
            return

        elif indexed == -1: # Page not exists, return = -1
            id_new_page = self.insertPage(url)

        elif indexed > 0: # Page exists without registered words, so return Page ID
            id_new_page = indexed

        print('Indexando ' + url)

        text = self.getText(soap)
        words = self.separateWords(text)

        for i in range(len(words)):
            current_word = words[i]
            id_word = self.indexedWord(current_word)
            
            if id_word == -1: # Word not exists
                id_word = self.insertWord(current_word)
            
            self.insertWordLocation(id_new_page, id_word, i)
        

class Searcher():
    def __init__(self, search, score_mode='distance'):
        lines, words_id = self.searchWords(search)
        if score_mode.lower() == 'distance':
            scores = self.setScoreByDistance(lines)
        elif score_mode.lower() == 'frequency':
            scores = self.setScoreByFrequency(lines)
        else:
            print("Set Score by 'Distance' or 'Frequecy'")
        ordered_scores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, idurl) in ordered_scores[:10]:
            print('%f\t%s' % (score, self.getUrl(idurl)))

    def getWordId(self, word):
        _return = -1
        stemmer = nltk.stem.RSLPStemmer() # RSLPStemmer() -> stemmer for Brazil
        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db')
        cursor = connection.cursor()
        
        cursor.execute('select idword from words where word = %s', stemmer.stem(word))
        if cursor.rowcount > 0:
            _return = cursor.fetchone()[0]

        cursor.close()
        connection.close()

        return _return


    def searchOneWord(self, word):
        word_id = self.getWordId(word)
        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db')
        cursor = connection.cursor()

        cursor.execute('select urls.url from word_location plc inner join urls on plc.idurl = urls.idurl where plc.idword = %s', word_id)

        pages = set()
        for url in cursor:
            pages.add(url[0])

        cursor.close()
        connection.close()

        return pages


    def searchWords(self, search):
        camps_list = 'p1.idurl'
        tables_list = ''
        terms_list = ''
        words_id = []

        words = search.split(' ')

        table_num = 1
        for word in words:
            word_id = self.getWordId(word)
            if word_id > 0:
                words_id.append(word_id)
                if table_num > 1:
                    tables_list += ', '
                    terms_list += ' and '
                    terms_list += 'p%d.idurl = p%d.idurl and ' %(table_num-1, table_num)
                camps_list += ', p%d.location' % table_num
                tables_list += ' word_location p%d' % table_num
                terms_list += 'p%d.idword = %d' % (table_num, word_id)
                table_num += 1

        complete_search = 'select %s from %s where %s' % (camps_list, tables_list, terms_list)

        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db')
        cursor = connection.cursor()

        cursor.execute(complete_search)
        lines = [line for line in cursor]

        cursor.close()
        connection.close()

        return lines, word_id


    def getUrl(self, idurl):
        _return = ''
        connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db')
        cursor = connection.cursor()

        cursor.execute('select url from urls where idurl = %s', idurl)
        if cursor.rowcount > 0:
            _return = cursor.fetchone()[0]
        
        cursor.close()
        connection.close()
        
        return _return

    def setScoreByFrequency(self, lines):
        count = dict([line[0], 0] for line in lines)
        for line in lines:
            count[line[0]] += 1
        
        return self.normalize(count)

    def setScoreByDistance(self, lines):
        if len(lines[0]) <= 2:
            return dict([(line[0], 1.0) for line in lines])
        distance = dict([(line[0], 1000000) for line in lines])
        for line in lines:
            dist = sum([abs(line[i] - line[i-1]) for i in range(2, len(line))])
            if dist < distance[line[0]]:
                distance[line[0]] = dist
            
        return self.normalize(distance)


    def normalize(self, notes): # -> type(notes) = dict
        _min = 0.00001
        _max = max(notes.values())
        if _max == 0:
            _max = _min

        return dict([(id, float(note) / _max) for (id, note) in notes.items()])
     

if __name__ == '__main__':
    my_link_list = ['https://pt.wikipedia.org/wiki/Linguagem_de_programação']
    #craw = Crawler(my_link_list, depth=2)
    craw = Searcher('')