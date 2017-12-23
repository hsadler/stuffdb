# -*- coding: utf-8 -*-

# MySQL test data stuffing tool


import sys
import MySQLdb as mdb

import pprint
pp = pprint.PrettyPrinter(indent=4)


# mock data
options = {
	"database": "testdb",
	"password": "",
	"table": "stuff_test_1",
	"columns": [
		{
			"name": "col_1",
			"datatype": "int",
			"constraint": "PRIMARY KEY",
			"density": 33
		},
		{
			"name": "col_2",
			"datatype": "varchar(225)",
			"constraint": "NOT NULL",
			"density": 0
		},
	],
	"count": 10000
}


# setup db connection
connection = mdb.connect(
	host='localhost',
	user='root',
	db=options['database'],
	passwd=options['password']
)
cur = connection.cursor(mdb.cursors.DictCursor)
connection.set_character_set('utf8')
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')


# db interface methods
def get_tables(cur):
	cur.execute("""SHOW TABLES;""")
	tables = [
		table['Tables_in_{}'.format(options['database'])]
		for table in cur.fetchall()
	]
	print 'tables: ' + str(tables)
	return tables

def create_table(cur, table, column_info):
	print 'creating table: ' + table

def insert_to_table(cur, table, columns, insert_values):
	print 'inserting to table: ' + table


# data generation methods
def get_integer_insert_values(count, density):
	pass

def get_string_insert_values(count, density):
	pass


# do the script stuff
with connection:

	# get current table list
	tables = get_tables(cur=cur)

	# create table if does not exist
	if options['table'] not in tables:
		create_table(
			cur=cur,
			table=options['table'],
			column_info=None
		)

	# insert test data
	insert_to_table(
		cur=cur,
		table=options['table'],
		columns=None,
		insert_values=None
	)


# example stuffing query:

# INSERT INTO table_name
# 	(col_1_name, col_2_name, col_3_name)
# VALUES
# 	('string_1', 'string_2', 'string_3'),
# 	('string_1', 'string_2', 'string_3'),
# 	('string_1', 'string_2', 'string_3'),
# 	('string_1', 'string_2', 'string_3'),
# 	('string_1', 'string_2', 'string_3'),
# 	('string_1', 'string_2', 'string_3');


# test query execution
# with connection:
#     cur.execute("""
#         SELECT table_name AS "Table",
#         ROUND(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)"
#         FROM information_schema.TABLES
#         WHERE table_schema = "{db_name}"
#         ORDER BY (data_length + index_length) DESC;
#     """.format(db_name=options['database']))

# print 'db test connection:'
# pp.pprint(cur.fetchall())


