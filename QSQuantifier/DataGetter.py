#!/usr/bin python
# coding=utf-8

# This Module provides some functions about getting stock's data.
import DataConnection as dc
import datetime
import numpy as np
import db_func


LEAST_DATE = '2016-01-01 00:00:00'

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

			#if it is the loop to search day.
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

		# beyound threshold
		if temp < string_toDatetime(LEAST_DATE):
			print('Warning: the date you search surpass threshold - 2016-01-01 00:00:00')
			break;

		results = db_func.query_dataThreshold(collection,str(query_time),str(temp))
		# no skip
		if (skip_paused and unit_type==0 and results.count()==0) == False:
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
	opens = tuple_result[0]['newest'] 
	close = tuple_result[-1]['newest']

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


	avg = round(money/volume,2)

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
	

def attribute_history(security,current_time,count,unit='d',fields=['open','close','high_limit','low_limit','volume'],skip_paused=True,fq='pre'):

	db = dc.startConnection('192.168.69.54',27017)
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

	time_stack = []
	for k,v in cds:
		time_stack.append(k)
		make_statisticsByProperty(v,query_properties)

	# Show Search Time
	for x in xrange(len(time_stack)-1):
		print (time_stack.pop())

	query_result = {}

	# change all value to numpy.ndataArray
	for k,v in query_properties.iteritems():
		if len(cds)>1:
			v = v[0:-1]
		query_result[k] = np.array(v)

	return query_result
