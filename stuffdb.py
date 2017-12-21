# -*- coding: utf-8 -*-

# MySQL test data stuffing tool


import sys
import MySQLdb as mdb

import pprint
pp = pprint.PrettyPrinter(indent=4)


db_name = sys.argv[1]


# setup db connection
connection = mdb.connect(
	host='localhost',
	user='root',
	passwd='',
	db=db_name
)
cur = connection.cursor(mdb.cursors.DictCursor)
connection.set_character_set('utf8')
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')


# test query execution
with connection:
    cur.execute("""
        SELECT table_name AS "Table",
        ROUND(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)"
        FROM information_schema.TABLES
        WHERE table_schema = "{db_name}"
        ORDER BY (data_length + index_length) DESC;
    """.format(db_name=db_name))

print 'db test connection:'
pp.pprint(cur.fetchall())



