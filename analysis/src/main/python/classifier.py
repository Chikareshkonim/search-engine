from elasticsearch import Elasticsearch
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.datasets import fetch_20newsgroups
import socket
import threading

print("training started")
#training part
newsgroups_train = fetch_20newsgroups(subset='train')
groups = {group: 0 for group in newsgroups_train.target_names}
vectorizer = TfidfVectorizer()
vectors = vectorizer.fit_transform(newsgroups_train.data)
clf = MultinomialNB()
clf.fit(vectors, newsgroups_train.target)

def respond(conn):
	global groups
	conn.send(b'you are connected\n')
	isrunning = True
	while isrunning:
		msg = conn.recv(100).decode('utf-8')
		msg = msg[:len(msg) - 2]
		if groups.__contains__(msg):
			conn.send((str(groups[msg]) + '\n').encode('utf-8'))
		if msg == 'close':
			conn.close()
			isrunning = False

def listen():
	serversocket = socket.socket()
	serversocket.bind((socket.gethostname(), 8082))
	serversocket.listen()
	while True:
		conn, addr = serversocket.accept()
		threading.Thread(target=respond, args=(conn, )).start()
		
print("network started")
threading.Thread(target=listen, args=()).start()

print("scrolling started")
#scrolling elastic data
es = Elasticsearch(['s1:9200', 's2:9200', 's3:9200'])
page = es.search(index='pages', doc_type='_doc', scroll='2m', size=10)
sid = page['_scroll_id']
counter = len(page['hits']['hits'])
while len(page['hits']['hits']) > 0:
	page = es.scroll(scroll_id=sid, scroll='2m')
	#for doc in page['hits']['hits']:
		#index = clf.predict(vectorizer.transform([doc['_source']['content']]))[0]
		#print(newsgroups_train.target_names[index])
	counter += len(page['hits']['hits'])
	docs = [doc['_source']['content'] for doc in page['hits']['hits']]
	group_list = [newsgroups_train.target_names[index] for index in clf.predict(vectorizer.transform(docs))]
	for group in group_list:
		groups[group] += 1
	sid = page['_scroll_id']
