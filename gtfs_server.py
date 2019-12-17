#!/usr/bin/env python3

import sqlite3
import json
import math

###
### Flask Stuff
###

from flask import Flask, escape, request, g, Response, session
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
	return Response(json.dumps(obj), mimetype = "application/json")

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

class SQLPair:
	def __init__(self, sql, params={}):
		self.params = params
		self.sql = sql

	def exec(self, db):
		cursor = db.cursor()
		cursor.execute(self.sql, self.params)
		return [dict(row) for row in cursor.fetchall()]

def sql_id(table, id_field, id):
	sql = f"SELECT * FROM {table} WHERE {id_field} = :id"
	return SQLPair(sql, {"id": id})

def sql_list(table, count, page):
	params = {"limit": count, "offset": page * count}
	sql = f"SELECT * FROM {table} LIMIT :limit OFFSET :offset"
	return SQLPair(sql, params)

def sql_find(table, search_fields, search):
	where = " OR ".join([f"{sf} LIKE '%'||:search||'%'" for sf in search_fields])
	sql = f"SELECT * FROM {table} WHERE {where} LIMIT :limit OFFSET :page"

	params = sql_list(table).params
	params.update({'search': search})

	return SQLPair(sql, params)

def sql_locate(table, km, lat, lon, count, page):
	high_lat = lat + kilometer_to_lat(km)
	high_lon = lon + kilometer_to_lon(lat, km)
	low_lat = lat - kilometer_to_lat(km)
	low_lon = lon - kilometer_to_lon(lat, km)

	list_params = sql_list(table, count, page).params
	locate_params = {"high_lat": high_lat, "high_lon": high_lon, "low_lat": low_lat, "low_lon": low_lon}
	params = {**list_params, **locate_params}

	sql = f'''SELECT * FROM {table}
		WHERE stop_lat < :high_lat AND stop_lat > :low_lat
		AND stop_lon < :high_lon AND stop_lon > :low_lon
		LIMIT :limit OFFSET :offset'''

	return SQLPair(sql, params)

def sql_fetch(table):
	return SQLPair(f"SELECT * FROM {table}")

def sql_row_count(table):
	#return SQLPair(f"SELECT count(*) AS row_count FROM {table}")
	return SQLPair(f"SELECT MAX(_ROWID_) AS row_count FROM {table}")

def sql_schema(table):
	return SQLPair(f"PRAGMA table_info({table})")

def sql_table_names():
	return SQLPair("SELECT name FROM sqlite_master WHERE type = 'table'")

def table_info(table, db):
	row_count = sql_row_count(table).exec(db)[0]['row_count']
	schema = sql_schema(table).exec(db)
	return {
		'name' : table,
		'verbs' : VERBS[table],
		'row_count': row_count,
		'schema': schema
	}

###
### API Definitions
###

# the tables that exist in the DB we are serving
TABLES = [row['name'] for row in sql_table_names().exec(get_db())]

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
		raise ParamError("Invalid parameter value")

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
	count = get_param_numeric('count', int, 0, 100, 25)
	return (lat, lon, count)

@app.route('/api/<table>/find/<search>')
def route_find(table, search):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('find' in VERBS[table], Error.NO_VERB)

	search_fields = SEARCH_FIELDS[table]

	sqlpair = sql_find(table, search_fields, search)

	return json_response(sqlpair.exec(g.db))

@app.route('/api/<table>/id/<id>')
def route_id(table, id):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('id' in VERBS[table], Error.NO_VERB)

	# This is Kind of a hack but does satisfy GTFS spec
	id_field = table.rstrip('s') + '_id'
	sqlpair = sql_id(table, id_field, id)

	return json_response(sqlpair.exec(g.db))

@app.route('/api/<table>/list')
def route_list(table):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('list' in VERBS[table], Error.NO_VERB)

	(count, page) = get_list_params()

	return json_response(sql_list(table, count, page).exec(g.db))

@app.route('/api/<table>')
def route_table(table):
	api_assert(table in TABLES, Error.NO_TABLE)
	return json_response(table_info(table, g.db))

@app.route('/api/<table>/fetch')
def route_fetch(table):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('fetch' in VERBS[table], Error.NO_VERB)

	return json_response(sql_fetch(table).exec(g.db)[0])

@app.route('/api/<table>/locate')
def route_locate(table):
	api_assert(table in TABLES, Error.NO_TABLE)
	api_assert('locate' in VERBS[table], Error.NO_VERB)
	for p in ['lat', 'lon', 'range']:
		api_assert(p in request.args, Error.NO_PARAM(p))

	(lat, lon, _range) = get_locate_params()

	return json_response(sql_locate(table, _range, lat, lon, count, page).exec(g.db))

@app.route('/api')
def route_api():
	return json_response([table_info(table, g.db) for table in TABLES])

@app.errorhandler(APIError)
@app.errorhandler(ParamError)
def handle_error(error):
	return error_json_response(str(error))
