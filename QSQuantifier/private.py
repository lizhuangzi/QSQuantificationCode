#!/usr/bin python
# coding=utf-8


from pymongo import MongoClient
import pymongo


# def drop_allCollections(db):
# 	rr = db.collection_names(False)
# 	for x in rr:
# 		db.drop_collection(x)


# def remove_illegalDocument(db):
# 	rr = db.collection_names(False)
# 	for x in rr:
# 		col = db[x]
# 		col.delete_many({"security_code":"证券代码"})
# 	print("delete illigal code finish")

# add Index for datatime.
def add_datetime_index(db):

	rr = db.collection_names(False)
	for x in rr:
		col = db[x]
		col.create_index('date_time')
	print("add index,finish")

# def drop_date_index(db):
# 	rr = db.collection_names(False)
#  	for x in rr:
#  		col = db[x]
#  		c = col.ensure_index('date_time')
#  		if len(c)>0:
#  			col.drop_index('date_time_1')
#  	print("drop index,finish")
