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

# add Index for datatime.
# def add_datetime_index(db):

# 	rr = db.collection_names(False)
# 	for x in rr:
# 		col = db[x]
# 		col.create_index('date_time')
# 	print("add index,finish")
