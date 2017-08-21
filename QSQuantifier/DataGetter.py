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
	

def serach_timelist(collection,query_time,unit,count,skip_paused=True,pretime=True):

	timedict = {}

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

	# if pretime:
	# 	count = count + 1
	i= 0; t=0;
	while i < count+1:

		if unit_type == 0:
			temp = query_time + datetime.timedelta(days = -1)

			if t == 0:

				tempstr = str(temp)
				strArr = tempstr.split(" ")
				newStr = strArr[0]+" "+"09:00:00"
				temp = string_toDatetime(newStr)
				query_time = string_toDatetime(strArr[0] +" "+"22:00:00")

		elif unit_type == 1:
			temp = query_time + datetime.timedelta(minutes = -1)
		else:
			temp = query_time + datetime.timedelta(seconds = -1)

		results = db_func.query_dataThreshold(collection,str(query_time),str(temp))

		# print('get records %s', results.count())

		# no skip
		if (skip_paused and unit_type==0 and len(tuple(results))==0) == False:
			i += 1

			timedict[temp] = tuple(results.clone())

		query_time = temp
		t += 1

	return timedict

def make_statisticsByProperty(tuple_result,query_properties={}):

	qpitks = query_properties.keys()

	if len(tuple_result) == 0:
		for v in query_properties.itervalues():
			v.append(0.0)
		return
	
	# x is a dictionary
	volume = 0.0
	money = 0.0
	opens = tuple_result[-1]['newest'] 
	close = tuple_result[0]['newest']

	hasmoney = 'money' in qpitks
	hasavg = 'avg' in qpitks
	haslow = 'low' in qpitks
	hashigh = 'high' in qpitks
	has_pre_close = 'pre_close' in qpitks


	high = 0.0
	low = 0.0

	avg = 0.0

	for x in tuple_result:
		volume += x['deal_amount']
		money += x['deal_money']

		if high<x['newest']:
			high = x['newest']
		if low>x['newest']:
			low = x['newest']


	avg = money/volume

	if hasmoney:
		query_properties['money'].insert(0,money)
	if hasavg:
		query_properties['avg'].insert(0,avg)
	if haslow:
		query_properties['low'].insert(0,low)
	if hashigh:
		query_properties['high'].insert(0,high)

	
	close_arr = query_properties['close']
	if len(query_properties['close'])!=0:
		last_close = close_arr[0]
		if has_pre_close:
			query_properties['pre_close'].insert(0,last_close)

		high_limit = round(last_close + last_close*0.1,2)
		low_limit = round(last_close - last_close*0.1,2)
		query_properties['high_limit'].insert(0,high_limit)
		query_properties['low_limit'].insert(0,low_limit)

	else:
		query_properties['high_limit'].insert(0,0.0)
		query_properties['low_limit'].insert(0,0.0)
		if has_pre_close:
			query_properties['pre_close'].insert(0,0.0)


	query_properties['close'].insert(0,close)
	query_properties['open'].insert(0,opens)

	query_properties['volume'].insert(0,volume)

	return close
		
def lasted_time_unitInfo(lasted_day_tuple):
	close = lasted_day_tuple[0]['newest']
	return

def attribute_history(security,current_time,count,unit='d',fields=['open','close','high_limit','low_limit','volume','money','avg','pre_close','paused'],skip_paused=True,fq='pre'):

	db = dc.startConnection()
	if db == None: 
		return
	# get collection from db by security
	collection = db[security]

	query_time= string_toDatetime(current_time)

	time_dict_result = serach_timelist(collection,query_time,unit,count,skip_paused)

	query_properties = {x:[] for x in fields}

	#sort data by date
	cds = time_dict_result.items()
	cds.sort()

	# cc = lasted_time_unitInfo(cds[-1])

	# cds.remove(-1)
	#making statistic
	for k,v in cds:
		print(k)
		make_statisticsByProperty(v,query_properties)


	query_result = {}

	# change all value to numpy.ndataArray
	for k,v in query_properties.iteritems():
		v = v[0:-1]
		query_result[k] = np.array(v)

	return query_result
