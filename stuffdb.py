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
			"cardinality": 3
		},
		{
			"name": "col_2",
			"datatype": "varchar(225)",
			"constraint": "NOT NULL",
			"cardinality": "Infinity"
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


# data generation methods
def generate_integer_insert_values(count, cardinality):
	curr = 0
	integer_set = range(0, cardinality)
	values = integer_set
	while len(values) < count:
		values += integer_set
	return values[0:count]

def generate_string_insert_values(count, cardinality):
	int_values = get_integer_insert_values(count, cardinality)
	return [ hex(val) for val in int_values ]


# query build method
def build_insert_query(table, *column_datas):

	"""
		Args:
			table: <str>
			*column_datas: {
				name: <str>
				insert_values: [
					<int, str>,
					...
				]
			}

		Returns:
			insert_query: <str>
	"""

	columns = []
	values = []

	# preset values inner arrays
	val_sets_length = min([ len(x['insert_values']) for x in column_datas ])
	for i in range(0, val_sets_length):
		values.append([])

	# build columns and values lists
	for column_data in column_datas:
		columns.append(column_data['name'])
		for i in range(0, val_sets_length):
			val = column_data['insert_values'][i]
			values[i].append(val)

	table_name = table
	columns_str = '(' + ",".join(columns) + ')'
	# build values string
	values_str = []
	for val_list in values:
		val_set_str = []
		for v in val_list:
			if isinstance(v, str):
				val_set_str.append('"' + v + '"')
			else:
				val_set_str.append(str(v))
		values_str.append('(' + ','.join(val_set_str) + ')')
	values_str = ','.join(values_str)

	insert_query = """
		INSERT INTO {table_name}
			{columns_str}
		VALUES
			{values_str};
	""".format(
		table_name=table_name,
		columns_str=columns_str,
		values_str=values_str
	)

	return insert_query


# TESTING:
query_string = build_insert_query(
	'table',
	{
		"name": 'insert-integers',
		"insert_values": [1, 2, 3]
	},
	{
		"name": 'insert-strings',
		"insert_values": ['a', 'b', 'c']
	}
)

print query_string

# should output:
	# INSERT INTO table
	# 	(insert-integers, insert-strings)
	# VALUES
	# 	(1, 'a'),
	# 	(2, 'b'),
	# 	(3, 'c')


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


# do the db table creation and inserts
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


