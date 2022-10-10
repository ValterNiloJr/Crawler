import crawler
import searcher
import db

my_link_list = ['https://pt.wikipedia.org/wiki/Linguagem_de_programação']

db.Crawler(my_link_list, depth=2)
db.Seacher()
