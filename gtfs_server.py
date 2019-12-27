#!/usr/bin/env python3

import sqlite3
import json
import math
import time

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
	headers = {}
	if app.env == 'development':
		headers = {"Access-Control-Allow-Origin": "*"}
	return Response(json.dumps(obj), mimetype="application/json", headers=headers)

def error_json_response(message):
	return json_response({"error": message})

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
	count = get_param_numeric('count', int, 0, 300, 25)
	page = get_param_numeric('page', int, 0, float("+inf"), 0)
	return (count, page)

def get_locate_params():
	high_lat = get_param_numeric('high_lat', float, -90.0, 90.0, None)
	low_lat = get_param_numeric('low_lat', float, -90.0, 90.0, None)
	high_lon = get_param_numeric('high_lon', float, -180.0, 180.0, None)
	low_lon = get_param_numeric('low_lon', float, -180.0, 180.0, None)
	return (high_lat, low_lat, high_lon, low_lon)

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

@app.route('/api/routes/list')
def route_routes_list():
	sql = '''
	SELECT DISTINCT
		r.route_id, r.route_short_name, r.route_long_name, r.route_desc,
		t.trip_headsign, t.shape_id, t.direction_id
	FROM routes r
	INNER JOIN trips t ON r.route_id = t.route_id
	ORDER BY r.route_short_name, t.trip_headsign, r.route_id'''
	return json_response(sql_query(sql, g.db))

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

	for param in ['high_lat', 'low_lat', 'high_lon', 'low_lon']:
		api_assert(param in request.args, Error.NO_PARAM(param))

	(high_lat, low_lat, high_lon, low_lon) = get_locate_params()
	(count, page) = get_list_params()

	params = {
		"high_lat": high_lat, "low_lat": low_lat,
		"high_lon": high_lon, "low_lon": low_lon,
		"limit": count,
		"offset": page * count
	}

	sql = f'''SELECT * FROM {table}
		WHERE stop_lat < :high_lat AND stop_lat > :low_lat
		AND stop_lon < :high_lon AND stop_lon > :low_lon
		LIMIT :limit OFFSET :offset'''

	return json_response(sql_query(sql, g.db, params))


@app.route('/api/stop_times/<stop_id>/schedule')
def route_schedule(stop_id):
	yyyymmdd = request.args.get('date', None)
	if yyyymmdd is None:
		return json_response([])

	weekday_index = 0
	try:
		weekday_index = time.strptime(yyyymmdd, '%Y%m%d').tm_wday
	except ValueError:
		return json_response([])

	weekday = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'][weekday_index]

	sql="""
	SELECT st.arrival_time, t.trip_headsign, r.route_short_name, r.route_long_name
	FROM trips t
	INNER JOIN stop_times st ON st.trip_id = t.trip_id
	INNER JOIN routes r ON t.route_id = r.route_id
	WHERE
		st.stop_id = :stop_id AND 
		t.service_id IN
			(SELECT c.service_id FROM calendar c
			WHERE
				c.end_date > :yyyymmdd
				AND c.{0} = 1
				AND NOT :yyyymmdd IN (SELECT date FROM calendar_dates cd WHERE cd.exception_type = 2)
			UNION
			SELECT service_id FROM calendar_dates
			WHERE
				date = :yyyymmdd AND exception_type = 1)
	ORDER BY st.arrival_time
	""".format(weekday)

	params = {'yyyymmdd': yyyymmdd, 'stop_id': stop_id}
	return json_response(sql_query(sql, g.db, params))

@app.route('/api/route/<shape_id>/geojson')
def route_geojson(shape_id):
	api_assert('shapes' in TABLES, Error.NO_TABLE)

	sql = '''SELECT s.* FROM shapes s
	WHERE s.shape_id = :shape_id
	ORDER BY s.shape_pt_sequence'''
	params = {'shape_id': shape_id}
	cursor = sql_query(sql, g.db, params)

#	sql = 'SELECT DISTINCT s.*
#	FROM trips t INNER JOIN shapes s ON s.shape_id = t.shape_id 
#	WHERE t.route_id = :route_id
#	ORDER BY s.shape_id, s.shape_pt_sequence'
#	params = {'route_id': route_id}
#	cursor = sql_query(sql, g.db, params)

	coordinates = []
	for row in cursor:
		coordinates.append([row['shape_pt_lon'], row['shape_pt_lat']])

	return json_response({
		'type': 'Feature',
		'geometry': { 'type': 'LineString', 'coordinates': coordinates }
	})


@app.route('/api')
def route_api():
	return json_response({'success': 'API Running'})

@app.errorhandler(APIError)
@app.errorhandler(ParamError)
def handle_error(error):
	return error_json_response(str(error))
