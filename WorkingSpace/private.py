#!/usr/bin python
# coding=utf-8


from pymongo import MongoClient
import pymongo



def connect_database(dbname = '',ip = 'localhost',port = 27017):
	hasConnected = False
	if hasConnected:
		print("hasConnected")
	else:
		conn = MongoClient('localhost',27017)
		db = conn[dbname]

		if db == None:
			print("db not exist")
		else:
			print("connect success")
			hasConnected = True
			
		return db



def add_datetime_index(db):

	rr = db.collection_names(False)
	for x in rr:
		col = db[x]
		col.create_index('date_time')
	print("add index,finish")
