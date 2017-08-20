#!/usr/bin python
# coding=utf-8

# This Module provides some functions about getting stock's data.
import DataConnection as dc
import datetime
import numpy as np
import db_func

def string_toDatetime(timestr):
	return datetime.datetime.strptime(timestr,"%Y-%m-%d %H:%M:%S")

def last_days(current_time,num = 1):
	return current_time + datetime.timedelta(days = -num)
	

def serach_timelist(query_time,unit,count):

	timelist = []
	unit_type = -1
	if unit == 'd':
		unit_type = 0
	elif unit == 'm':
		unit_type = 1
	elif unit == 's':
		unit_type = 2
	else:
		print('The parameter unit is illegal')
		return

	for i in range(count):

		if unit_type == 0:
			temp = query_time + datetime.timedelta(days = -(i+1))
		elif unit_type == 1:
			temp = query_time + datetime.timedelta(minutes = -(i+1))
		else:
			temp = query_time + datetime.timedelta(seconds = -(i+1))

		timelist.append(temp)

	return tuple(timelist)

def make_statisticsByProperty(tuple_result,query_properties):

	if len(tuple_result) == 0:
		for v in query_properties.itervalues():
			v.append(0.0)
		return
	
	# x is a dictionary
	volume = 0.0
	money = 0.0
	close = tuple_result[0]['new']
	for x in tuple_result:
		volume += x['da']
		money += x['dm']

	query_properties['close'].append(close)
	query_properties['money'].append(money)
	query_properties['volume'].append(volume)
		

def attribute_history(security,current_time,count,unit='d',fields=['close','high_limit','low_limit','volume','money'],skip_paused=True,fq='pre'):

	db = dc.startConnection()
	if db == None: 
		return
	# get collection from db by security
	collection = db[security]

	query_time= string_toDatetime(current_time)
	time_tuple = serach_timelist(query_time,unit,count)

	query_properties = {x:[] for x in fields}
	
	for sd in time_tuple:

		print(sd)
		results = db_func.query_dataThreshold(collection,str(query_time),str(sd))

		# it is a date that the stock paused
		while skip_paused and unit=='d' and len(tuple(results))==0:
			next_sd = last_days(sd)
			results = db_func.query_dataThreshold(collection,str(query_time),str(next_sd))

		tuple_result = tuple(results)
		make_statisticsByProperty(tuple_result,query_properties)

	query_result = {}

	# change all value to numpy.ndataArray
	for k,v in query_properties.iteritems():
		query_result[k] = np.array(v)

	return query_result
