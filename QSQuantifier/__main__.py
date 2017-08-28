#!/usr/bin python
# coding=utf-8
# import private
# import DataConnection as dc

# db = dc.startConnection()

# private.add_datetime_index(db)

# private.remove_illegalDocument(db)

# private.drop_date_index(db)

import DataGetter as dg

timeStr = "2016-01-29 10:00:00"

query_result = dg.attribute_history('sh600000', timeStr, 20, 'd',fields=['open','close','high_limit','low_limit','volume','avg','pre_close'])

print(query_result)