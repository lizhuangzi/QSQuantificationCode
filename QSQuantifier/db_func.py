#!/usr/bin python
# coding=utf-8
import pymongo

def query_dataThreshold(col,bigtime,smalltime,InorOut=0):

	if InorOut == 0:
		results = col.find({"date_time":{"$lt":str(bigtime),"$gt":str(smalltime)}})
	else:
		results = col.find({"date_time":{"$lt":str(smalltime),"$gt":str(bigtime)}})


	return results