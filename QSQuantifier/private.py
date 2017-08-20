#!/usr/bin python
# coding=utf-8


from pymongo import MongoClient
import pymongo



# add Index for datatime.
# def add_datetime_index(db):

# 	rr = db.collection_names(False)
# 	for x in rr:
# 		col = db[x]
# 		col.create_index('date_time')
# 	print("add index,finish")
