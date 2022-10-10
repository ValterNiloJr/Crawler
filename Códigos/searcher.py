import pymysql
import nltk

def getWordId(word):
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


def searchOneWord(word):
    word_id = getWordId(word)
    connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db')
    cursor = connection.cursor()

    cursor.execute('select urls.url from word_location plc inner join urls on plc.idurl = urls.idurl where plc.idword = %s', word_id)

    pages = set()
    for url in cursor:
        pages.add(url[0])

    cursor.close()
    connection.close()

    return pages


def searchWords(search):
    camps_list = 'p1.idurl'
    tables_list = ''
    terms_list = ''
    words_id = []

    words = search.split(' ')

    table_num = 1
    for word in words:
        word_id = getWordId(word)
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


def getUrl(idurl):
    _return = ''
    connection = pymysql.connect(host='localhost', user='root', passwd='user123', db='index_db')
    cursor = connection.cursor()

    cursor.execute('select url from urls where idurl = %s', idurl)
    if cursor.rowcount > 0:
        _return = cursor.fetchone()[0]
    
    cursor.close()
    connection.close()
    
    return _return

def setScoreByFrequency(lines):
    count = dict([line[0], 0] for line in lines)
    for line in lines:
        count[line[0]] += 1
    
    return normalize(count)

def setScoreByDistance(lines):
    if len(lines[0]) <= 2:
        return dict([(line[0], 1.0) for line in lines])
    distance = dict([(line[0], 1000000) for line in lines])
    for line in lines:
        dist = sum([abs(line[i] - line[i-1]) for i in range(2, len(line))])
        if dist < distance[line[0]]:
            distance[line[0]] = dist
        
    return normalize(distance)


def normalize(notes): # -> type(notes) = dict
    _min = 0.00001
    _max = max(notes.values())
    if _max == 0:
        _max = _min

    return dict([(id, float(note) / _max) for (id, note) in notes.items()])


def search(search, score_mode='distance'):
    lines, words_id = searchWords(search)
    if score_mode.lower() == 'distance':
        scores = setScoreByDistance(lines)
    elif score_mode.lower() == 'frequency':
        scores = setScoreByFrequency(lines)
    else:
        print("Set Score by 'Distance' or 'Frequecy'")
    ordered_scores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
    for (score, idurl) in ordered_scores[:10]:
        print('%f\t%s' % (score, getUrl(idurl)))

    
