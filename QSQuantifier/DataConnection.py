#!/usr/bin python
# coding=utf-8

from pymongo import MongoClient

def startConnection(ip = 'localhost',port = 27017,dbname = 'StockDatas'):

	client = MongoClient('192.168.69.54',27017)
	# try:
	# 	client.admin.command('ismaster')
	# except Exception as e:
	# 	print('Server not available')
	
	

	db = client[dbname]
	# print(db.collection_names(False))
	if db == None:
		print("db not exist")
	else:
		print("connect success")
	return db
