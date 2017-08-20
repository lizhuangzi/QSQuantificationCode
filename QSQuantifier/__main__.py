#!/usr/bin python
# coding=utf-8

import DataGetter as dg

timeStr = "2016-04-05 10:00:00"

query_result = dg.attribute_history('sz000005', timeStr, 2, 'm')

print(query_result)

# conn = MongoClient('localhost',27017)

# db = conn['StockDatas']

# rr = db.collection_names(False)

# db = private.connect_database('StockDatas')



# rr = db.collection_names(False)
# rrg = tuple(rr)

# for x in rrg:
# 	col = db[x]
# 	result = col.find({"date_time":{"$lt":"2016-01-05 09:30:03","$gt":"2016-01-04 09:30:03"}})
# 	print(result.count())
# 	del(result)

# i = 0
# while i<len(rrg):
# 	col = db[rrg[i]]
# 	result = col.find({"date_time":{"$lt":"2016-01-05 09:30:03","$gt":"2016-01-04 09:30:03"}})
# 	print(result.count())
# 	del(result)
# 	i += 1
	

# print "*****over"

# collection = db['sh600000']

# arr = collection.find({"deal_money":{"$lt":8000,"$gt":6000}})

#print(list(arr))