#!/usr/bin/env python3

import sqlite3
import json
import math

###
### Flask Stuff
###

from flask import Flask, request, g, Response
app = Flask(__name__)
app.config.from_pyfile('config.py')

def get_db():
	conn = sqlite3.connect(app.config['DATABASE'])
	conn.row_factory = sqlite3.Row
	return conn

@app.before_request
def before_request():
	g.db = get_db()

@app.teardown_request
def teardown_request(exception):
	if hasattr(g, 'db'):
		g.db.close()

###
### Utils
###

def json_response(obj):
	return Response(json.dumps(obj), mimetype="application/json")

def error_json_response(message):
	return json_response({"error": message})

def kilometer_to_lat(km):
	return (1 / 110.574) * km

def kilometer_to_lon(lat, km):
	lat_radians = (lat * math.pi) / 180
	return (1 / (111.320 * math.cos(lat_radians))) * km

###
### Verb-SQL Helpers
###

class CursorListAdapter(list):
	""" An object that yields rows from cursor when accessed like a list. This
	is useful for treating a database query like a list of results while
	elements are in fact being fetched on-demand by the cursor.
	"""
	def __init__(self, cursor):
		self.cursor = cursor

	def __iter__(self):
		for row in self.cursor:
			yield dict(row)

	def __getitem__(self, n):
		if not type(n) is int:
			raise TypeError("Index must be an integer")

		# skip over n rows if n is not zero or negative
		if n >= 1:
			self.cursor.fetchmany(n)

		# return the immediate next row
		return dict(self.cursor.fetchone()) 

def sql_query(sql, db, params={}):
	cursor = db.cursor()
	cursor.execute(sql, params)
	return CursorListAdapter(cursor)

###
### API Definitions
###

# the tables that exist in the DB we are serving
TABLE_NAMES = sql_query("SELECT name FROM sqlite_master WHERE type = 'table'", get_db())
TABLES = [row['name'] for row in TABLE_NAMES]

# dict of gtfs tables and their supported API verbs/actions
VERBS = {
	'agency': ['id', 'list'],
	'stops': ['id', 'list', 'find', 'locate'],
	'routes': ['id', 'list', 'find'],
	'trips': ['id'],
	'stop_times': ['list', 'find'],
	'calendar': ['fetch'],
	'calendar_dates': ['fetch'],
	'fare_attributes': ['id', 'list'],
	'fare_rules': ['list'],
	'shapes': ['list', 'find'],
	'frequencies': ['list'],
	'transfers': ['list'],
	'pathways': ['id', 'list'],
	'levels': ['id', 'list'],
	'feed_info': ['fetch']
}

# the columns to search for tables that support find
SEARCH_FIELDS = {
	'stops': ['stop_name', 'stop_desc'],
	'routes': ['route_desc', 'route_long_name', 'route_short_name'],
	'stop_times': ['stop_headsign'],
	'shapes': ['shape_id']
}

class APIError(Exception):
	pass

class ParamError(Exception):
	pass

# Holds Error Messages
class Error:
	NO_TABLE = "Table does not exist"
	NO_VERB = "Verb not supported"
	NO_PARAM = lambda p: "Missing required parameter: " + p

def get_param_numeric(key, type_fn, lower, upper, default):
	try:
		value = type_fn(request.args.get(key, default))
		return max(lower, min(value, upper))
	except (ValueError, TypeError) as e:
		raise ParamError("Invalid parameter value: " + e)

def api_assert(test, msg):
	if type(test) is bool and test:
		return True
	else:
		raise APIError(msg)

def get_list_params():
	count = get_param_numeric('count', int, 0, 100, 25)
	page = get_param_numeric('page', int, 0, float("+inf"), 0)
	return (count, page)

def get_locate_params():
	lat = get_param_numeric('lat', float, -90.0, 90.0, None)
	lon = get_param_numeric('lon', float, -180.0, 180.0, None)
	km_range = get_param_numeric('range', int, 0, 100, 25)
	return (lat, lon, km_range)

@app.route('/api/<table>/find/<search>')
def route_find(table, search):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('find' in VERBS[table], Error.NO_VERB)

	search_fields = SEARCH_FIELDS[table]
	where_clause = " OR ".join([f"{sf} LIKE '%'||:search||'%'" for sf in search_fields])
	sql = f"SELECT * FROM {table} WHERE {where_clause} LIMIT :limit OFFSET :page"

	(count, page) = get_list_params()
	params = {"limit": count, "offset": page * count, "search": search}

	return json_response(sql_query(sql, g.db, params))

@app.route('/api/<table>/id/<id_value>')
def route_id(table, id_value):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('id' in VERBS[table], Error.NO_VERB)

	# Remove trailing 's' on table name and append '_id' to create a string
	# with the id column name. This is somewhat of a hack but it satisifies the
	# GTFS spec
	id_field = table.rstrip('s') + '_id'
	sql = f"SELECT * FROM {table} WHERE {id_field} = :id"
	params = {"id": id_value}

	return json_response(sql_query(sql, g.db, params))

@app.route('/api/<table>/list')
def route_list(table):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('list' in VERBS[table], Error.NO_VERB)

	(count, page) = get_list_params()

	params = {"limit": count, "offset": page * count}
	sql = f"SELECT * FROM {table} LIMIT :limit OFFSET :offset"

	return json_response(sql_query(sql, g.db, params))

@app.route('/api/<table>')
def route_table(table):
	api_assert(table in TABLES, Error.NO_TABLE)

	row_count = sql_query(f"SELECT MAX(_ROWID_) AS row_count FROM {table}", g.db)[0]
	schema = sql_query(f"PRAGMA table_info({table})", g.db)[0]

	return json_response({
		'name' : table,
		'verbs' : VERBS[table],
		'row_count': row_count,
		'schema': schema
	})

@app.route('/api/<table>/fetch')
def route_fetch(table):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('fetch' in VERBS[table], Error.NO_VERB)

	sql = f"SELECT * FROM {table}"
	return json_response(sql_query(sql, g.db)[0])

@app.route('/api/<table>/locate')
def route_locate(table):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('locate' in VERBS[table], Error.NO_VERB)

	for param in ['lat', 'lon', 'range']:
		api_assert(param in request.args, Error.NO_PARAM(param))

	(lat, lon, km_range) = get_locate_params()
	(count, page) = get_list_params()

	params = {
		"high_lat": lat + kilometer_to_lat(km_range),
		"high_lon": lon + kilometer_to_lon(lat, km_range),
		"low_lat": lat - kilometer_to_lat(km_range),
		"low_lon": lon - kilometer_to_lon(lat, km_range),
		"limit": count,
		"offset": page * count
	}

	sql = f'''SELECT * FROM {table}
		WHERE stop_lat < :high_lat AND stop_lat > :low_lat
		AND stop_lon < :high_lon AND stop_lon > :low_lon
		LIMIT :limit OFFSET :offset'''

	return json_response(sql_query(sql, g.db, params))

@app.errorhandler(APIError)
@app.errorhandler(ParamError)
def handle_error(error):
	return error_json_response(str(error))
