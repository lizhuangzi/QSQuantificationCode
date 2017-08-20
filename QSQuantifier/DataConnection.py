#!/usr/bin python
# coding=utf-8

from pymongo import MongoClient

def startConnection(ip = 'localhost',port = 27017,dbname = 'StockDatas'):
	conn = MongoClient('localhost',27017)
	db = conn[dbname]
	if db == None:
		print("db not exist")
	else:
		print("connect success")
	return db

