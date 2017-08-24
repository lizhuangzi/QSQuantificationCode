#!/usr/bin python
# coding=utf-8
import private
import DataConnection as dc

# db = dc.startConnection()

# private.add_datetime_index(db)

import DataGetter as dg

timeStr = "2016-01-05 10:00:00"

query_result = dg.attribute_history('sh600000', timeStr, 3, 'd',fields=['open','close','high_limit','low_limit','volume','avg'])

print(query_result)
