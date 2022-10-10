import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import nltk
#nltk.download()
import pymysql

class Crawler():
    def __init__(self) -> None:
        pass

    def Get(*words, location='web', depth=1):
        words = words[1:][0]
        print(words)

    def Post():
        pass


def insertWordLocation(idurl, idword, location):
    # for insert data into db, use autocommit
    connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db', autocommit=True)
    cursor = connection.cursor()

    cursor.execute('insert into word_location (idurl, idword, location) values (%s, %s, %s)', (idurl, idword, location))
    id_word_location = cursor.lastrowid

    cursor.close()
    connection.close()

    return id_word_location


def insertWord(word):
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


def indexedWord(word):
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


def insertPage(url):
    # for insert data into db, use autocommit
    connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db', autocommit=True)
    cursor = connection.cursor()

    cursor.execute('insert into urls (url) values (%s)', url)

    id_page = cursor.lastrowid # only localhost

    cursor.close()
    connection.close()

    return id_page


def indexedPage(url):
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


def separateWords(text):
    stop = nltk.corpus.stopwords.words('portuguese')
    stemmer = nltk.stem.RSLPStemmer() # RSLPStemmer() -> stemmer for Brazil
    splitter = re.compile('\\W+')
    word_list = []
    aux_list = [word for word in splitter.split(text) if word != '']
    for word in aux_list:
        if word.lower() not in stop and len(word) > 1:
            word_list.append(stemmer.stem(word).lower())
    
    return word_list


def getText(soap):
    for tags in soap(['script', 'style']):
        tags.decompose()

    return ' '.join(soap.stripped_strings)


def indexer(url, soap):
    indexed = indexedPage(url)
    
    if indexed == -2: # Page exists with registered words
        return

    elif indexed == -1: # Page not exists, return = -1
        id_new_page = insertPage(url)

    elif indexed > 0: # Page exists without registered words, so return Page ID
        id_new_page = indexed

    print('Indexando ' + url)

    text = getText(soap)
    words = separateWords(text)

    for i in range(len(words)):
        current_word = words[i]
        id_word = indexedWord(current_word)
        
        if id_word == -1: # Word not exists
            id_word = insertWord(current_word)
        
        insertWordLocation(id_new_page, id_word, i)


def crawl(pages, depth=1):
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
            indexer(page, soap)
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


if __name__ == '__main__':
    my_link_list = ['https://pt.wikipedia.org/wiki/Linguagem_de_programação']
    crawl(my_link_list, depth=2)