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

def add_zero_forsomeproperties(query_properties):
	qpitks = query_properties.keys()
	has_pre_close = 'pre_close' in qpitks
	query_properties['high_limit'].append(0.0)
	query_properties['low_limit'].append(0.0)
	if has_pre_close:
		query_properties['pre_close'].append(0.0)
	

# make statistics for datas 
def make_statisticsByProperty(tuple_result,query_properties={},allcount=0):

	qpitks = query_properties.keys()

	# if len(tuple_result) == 0:
	# 	for v in query_properties.itervalues():
	# 		v.append(0.0)
	# 	return

	volume = 0.0
	money = 0.0
	opens = 0.0
	close = 0.0
	if len(tuple_result)!=0:
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

	if volume != 0:
		avg = round(money/volume,2)

	
	if hasmoney:
		query_properties['money'].append(money)
	if hasavg:
		query_properties['avg'].append(avg)
	if haslow:
		query_properties['low'].append(low)
	if hashigh:
		query_properties['high'].append(high)

	close_arr = query_properties['close']


	#if has yestady data
	if len(close_arr)!=0:
		last_close = close

		if has_pre_close:
			query_properties['pre_close'].append(last_close)

		high_limit = round(last_close + last_close*0.1,2)
		low_limit = round(last_close - last_close*0.1,2)

		query_properties['high_limit'].append(high_limit)
		query_properties['low_limit'].append(low_limit)


	query_properties['close'].append(close)
	query_properties['open'].append(opens)
	query_properties['volume'].append(volume)

	if len(close_arr) == allcount:
		add_zero_forsomeproperties(query_properties)


def serach_timelist(collection,query_time,unit,count,skip_paused=True,query_properties={}):

	time_list = []
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

	i= 0; t=0
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
			add_zero_forsomeproperties(query_properties)
			print('Warning: There is no early datas')
			break;

		results = db_func.query_dataThreshold(collection,str(query_time),str(temp))

		# no skip
		if (skip_paused and unit_type==0 and results.count()==0) == False:
			i += 1
			time_list.append(temp)
			make_statisticsByProperty(tuple(results.clone()), query_properties,count+1)
			del(results)

		query_time = temp
		t += 1
		
	return time_list

	
def attribute_history(security,current_time,count,unit='d',fields=['open','close','high_limit','low_limit','volume'],skip_paused=True,fq='pre'):

	db = dc.startConnection('192.168.69.54',27017)
	if db == None: 
		return
	# get collection from db by security
	collection = db[security]

	#Convert 
	query_time= string_toDatetime(current_time)
 	
 	query_properties = {x:[] for x in fields}

 	#Return a dictionary key:datetime,value:tuple(query datas)
	cds = serach_timelist(collection,query_time,unit,count,skip_paused,query_properties)

	#show Search days
	for x in xrange(0, count if (len(cds)==count+1) else len(cds)):
		print "Searching time is %s" %cds[x]

	#finall result dictionary
	query_result = {}

	# change all value to numpy.ndataArray
	for k,v in query_properties.iteritems():
		if len(cds)==count+1:
			v = v[0:-1]
		query_result[k] = np.array(v)

	return query_result
