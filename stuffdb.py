# -*- coding: utf-8 -*-

# MySQL test data stuffing tool

import sys
import MySQLdb as mdb

import pprint
pp = pprint.PrettyPrinter(indent=4)



# mock data
options = {
	"host": "localhost",
	"user": "root",
	"password": "",
	"database": "testdb",
	"table": "my_table",
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
		{
			"name": "col_3",
			"datatype": "varchar(225)",
			"distribution": 20
		},
	],
	"count": 10000
}



# constants
 datatype_map= {
 	'string': [
 		'CHAR', 'VARCHAR', 'BINARY', 'VARBINARY', 'BLOB', 'TEXT', 'ENUM', 'SET'
	],
	'int': [
		'INTEGER', 'INT', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'BIGINT'
	]
 }



# setup db connection
connection = mdb.connect(
	host=options['host'],
	user=options['user'],
	db=options['database'],
	passwd=options['password']
)
cur = connection.cursor(mdb.cursors.DictCursor)
connection.set_character_set('utf8')
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')



# helpers
def get_simple_datatype_from_mysql_datatype(mysql_datatype):
	for key, val in datatype_map.iteritems():
		if mysql_datatype in val:
			return key
	return None


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



# query string builder methods
def build_create_table_query(table_name, column_schema):

	"""
		Args:
			table_name: <str>
			column_schema:
			[
				{
					"name": <str>,
					"datatype": <str>,
					"constraint": <str>
				},
				...
			]
		Returns:
			insert_query: <str>
	"""

	columns_str = ','.join([
		(
			col['name'] +
			' ' + col['datatype'] +
			(' ' + col['constraint'] if 'constraint' in col else '')
		)
		for col in column_schema
	])

	create_table_query = """
		CREATE TABLE {table_name} (
			{columns_str}
		);
	""".format(
		table_name=table_name,
		columns_str=columns_str
	)

	return create_table_query


def build_insert_query(table_name, column_datas):

	"""
		Args:
			table: <str>
			column_datas: [
				{
					"name": <str>
					"insert_values": [
						<int || str>,
						...
					]
				},
				...
			]
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



# db interface methods
def get_tables(cur):
	cur.execute("""SHOW TABLES;""")
	tables = [
		table['Tables_in_{}'.format(options['database'])]
		for table in cur.fetchall()
	]
	return tables


def create_table(cur, table_name, column_schema):
	create_table_query = build_create_table_query(
		table_name=table_name,
		column_schema=column_schema
	)
	cur.execute(create_table_query)


def insert_to_table(cur, table_name, column_insert_data, count, batch_count):

	# TODOs for this function:
	# 	- consider datatype, count, and cardinality/distribution
	#	- create value lists per column
	#	- get insert query string
	#	- execute insert query

	# EXAMPLE column_insert_data:
	# [
	# 	{
	# 		"name": "col_1",
	# 		"datatype": "int",
	# 		"constraint": "PRIMARY KEY",
	# 		"cardinality": 3
	# 	},
	# 	{
	# 		"name": "col_2",
	# 		"datatype": "varchar(225)",
	# 		"constraint": "NOT NULL",
	# 		"cardinality": "Infinity"
	# 	},
	# 	{
	# 		"name": "col_3",
	# 		"datatype": "varchar(225)",
	# 		"distribution": 20
	# 	},
	# ]

	# EXAMPLE BUILD INSERT QUERY CALL:
	# build_insert_query(
	# 	table_name='table',
	# 	column_datas=[
	# 		{
	# 			"name": 'insert_integers',
	# 			"insert_values": [1, 2, 3]
	# 		},
	# 		{
	# 			"name": 'insert_strings',
	# 			"insert_values": ['a', 'b', 'c']
	# 		}
	# 	]
	# )

	column_datas = []
	for insert_data in column_insert_data:
		values = []
		simple_datatype = get_simple_datatype_from_mysql_datatype(
			insert_data['datatype']
		)
		if simple_datatype === 'int':
			values = generate_integer_insert_values(count, cardinality)
		elif simple_datatype === 'string':
			values = generate_string_insert_values(count, cardinality)
		col_data = {
			'name': insert_data['name'],
			'insert_values': values
		}
		column_datas.append(col_data)

	insert_query = build_insert_query(
		table_name=table_name,
		column_datas=column_datas
	)

	cur.execute(insert_query)



# do the db table creation and inserts
with connection:

	# get current table list
	tables = get_tables(cur=cur)
	print 'tables: ' + str(tables)

	# create table if does not exist
	if options['table'] not in tables:
		print 'creating table: ' + options['table']
		create_table(
			cur=cur,
			table_name=options['table'],
			column_schema=options['columns']
		)

	# insert test data
	print 'inserting data to table: ' + options['table']
	insert_to_table(
		cur=cur,
		table_name=options['table'],
		column_insert_data=options['columns'],
		count=options['count'],
		batch_count=100 # TODO: hardcoded for now
	)



# TESTING:
query_string = build_create_table_query(
	table_name="my_table",
	column_schema=[
		{
			"name": "col_1",
			"datatype": "int",
			"constraint": "PRIMARY KEY"
		},
		{
			"name": "col_2",
			"datatype": "varchar(225)",
			"constraint": "NOT NULL"
		},
		{
			"name": "col_3",
			"datatype": "int"
		}
	]
)
print query_string
# should output:
	# CREATE TABLE my_table (
	# 	col_1 int PRIMARY KEY,
	# 	col_2 varchar(225) NOT NULL
	# )


query_string = build_insert_query(
	table_name='table',
	column_datas=[
		{
			"name": 'insert_integers',
			"insert_values": [1, 2, 3]
		},
		{
			"name": 'insert_strings',
			"insert_values": ['a', 'b', 'c']
		}
	]
)
print query_string
# should output:
	# INSERT INTO table
	# 	(insert_integers, insert_strings)
	# VALUES
	# 	(1, 'a'),
	# 	(2, 'b'),
	# 	(3, 'c')
