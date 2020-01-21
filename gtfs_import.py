#!/usr/bin/env python3

import csv
import argparse
import sqlite3 as db
import zipfile
import sys
import re
import io

SQL_CREATE_AGENCY = """
CREATE TABLE agency(
	agency_id TEXT,
	agency_name TEXT NOT NULL,
	agency_url TEXT NOT NULL,
	agency_timezone TEXT NOT NULL,
	agency_lang TEXT,
	agency_phone TEXT,
	agency_fare_url TEXT,
	agency_email TEXT
)"""

SQL_CREATE_STOPS = """
CREATE TABLE stops(
	stop_id TEXT PRIMARY KEY NOT NULL,
	stop_code TEXT,
	stop_name TEXT,
	stop_desc TEXT,
	stop_lat REAL,
	stop_lon REAL,
	zone_id TEXT,
	stop_url TEXT,
	location_type TINYINT,
	parent_station TEXT,
	stop_timezone TEXT,
	wheelchair_boarding TINYINT,
	level_id TEXT,
	platform_code TEXT
)"""

SQL_CREATE_ROUTES = """
CREATE TABLE routes(
	route_id TEXT PRIMARY KEY NOT NULL,
	agency_id TEXT,
	route_short_name TEXT,
	route_long_name TEXT,
	route_desc TEXT,
	route_type TINYINT NOT NULL,
	route_url TEXT,
	route_color VARCHAR(6),
	route_text_color VARCHAR(6),
	route_sort_order TINYINT
)"""

SQL_CREATE_TRIPS = """
CREATE TABLE trips(
	route_id TEXT NOT NULL,
	service_id TEXT NOT NULL,
	trip_id TEXT PRIMARY KEY NOT NULL,
	trip_headsign TEXT,
	trip_short_name TEXT,
	direction_id TINYINT,
	block_id TEXT,
	shape_id TEXT,
	wheelchair_accessible TINYINT,
	bikes_allowed TINYINT
)"""

SQL_CREATE_STOP_TIMES = """
CREATE TABLE stop_times(
	trip_id TEXT NOT NULL,
	arrival_time TEXT,
	departure_time TEXT,
	stop_id TEXT NOT NULL,
	stop_sequence INTEGER NOT NULL,
	stop_headsign TEXT,
	pickup_type TINYINT,
	drop_off_type TINYINT,
	shape_dist_traveled REAL,
	timepoint TINYINT
)"""

SQL_CREATE_CALENDAR = """
CREATE TABLE calendar(
	service_id TEXT NOT NULL,
	monday TINYINT NOT NULL,
	tuesday TINYINT NOT NULL,
	wednesday TINYINT NOT NULL,
	thursday TINYINT NOT NULL,
	friday TINYINT NOT NULL,
	saturday TINYINT NOT NULL,
	sunday TINYINT NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE NOT NULL
)"""

SQL_CREATE_CALENDAR_DATES = """
CREATE TABLE calendar_dates(
	service_id TEXT NOT NULL,
	date DATE NOT NULL,
	exception_type TINYINT NOT NULL
)"""

SQL_CREATE_FARE_ATTRIBUTES = """
CREATE TABLE fare_attributes(
	fare_id TEXT PRIMARY KEY NOT NULL,
	price REAL NOT NULL,
	currency_type TEXT NOT NULL,
	payment_method TINYINT NOT NULL,
	transfers TINYINT NOT NULL,
	agency_id TEXT,
	transfer_duration INTEGER
)"""

SQL_CREATE_FARE_RULES = """
CREATE TABLE fare_rules(
	fare_id TEXT NOT NULL,
	route_id TEXT,
	origin_id TEXT,
	destination_id TEXT,
	contains_id TEXT
)"""

SQL_CREATE_SHAPES = """
CREATE TABLE shapes(
	shape_id TEXT NOT NULL,
	shape_pt_lat REAL NOT NULL,
	shape_pt_lon REAL NOT NULL,
	shape_pt_sequence INTEGER NOT NULL,
	shape_dist_traveled REAL
)"""

SQL_CREATE_FREQUENCIES = """
CREATE TABLE frequencies(
	trip_id TEXT NOT NULL,
	start_time TEXT NOT NULL,
	end_time TEXT NOT NULL,
	headway_secs INTEGER NOT NULL,
	exact_times TINYINT
)"""

SQL_CREATE_TRANSFERS = """
CREATE TABLE transfers(
	from_stop_id TEXT NOT NULL,
	to_stop_id TEXT NOT NULL,
	transfer_type TINYINT NOT NULL,
	min_transfer_time INTEGER
)"""

SQL_CREATE_PATHWAYS = """
CREATE TABLE pathways(
	pathway_id TEXT NOT NULL,
	from_stop_id TEXT NOT NULL,
	to_stop_id TEXT NOT NULL,
	pathway_mode TINYINT NOT NULL,
	is_bidirectional TINYINT NOT NULL,
	length REAL,
	traversal_time INTEGER,
	stair_count INTEGER,
	max_slope REAL,
	min_width REAL,
	signposted_as TEXT,
	reversed_signposted_as TEXT
)"""

SQL_CREATE_LEVELS = """
CREATE TABLE levels(
	level_id TEXT PRIMARY KEY NOT NULL,
	level_index FLOAT NOT NULL,
	level_name TEXT
)"""

SQL_CREATE_FEED_INFO = """
CREATE TABLE feed_info(
	feed_publisher_name TEXT NOT NULL,
	feed_publisher_url TEXT NOT NULL,
	feed_lang TEXT NOT NULL,
	feed_start_date DATE,
	feed_end_date DATE,
	feed_version TEXT,
	feed_contact_email TEXT,
	feed_contact_url TEXT
)"""

SQL_LIST_CREATE_INDEXES = [
	('stops_index', 'CREATE INDEX stops_index ON stops(stop_code)'),
	('trips_index', 'CREATE INDEX trips_index ON trips(trip_id, route_id, shape_id, service_id)'),
	('stop_times_index', 'CREATE INDEX stop_times_index ON stop_times(trip_id, stop_id)'),
	('shapes_index', 'CREATE INDEX shapes_index ON shapes(shape_id)'),
]

def create_index(index_name, create_sql, conn):
	# Drop old index if exists
	conn.execute(f"DROP INDEX IF EXISTS {index_name}")
	conn.execute(create_sql)

def create_gtfs_table(table_name, create_sql, conn):
	# Drop old and create new table
	conn.execute(f"DROP TABLE IF EXISTS {table_name}")
	conn.execute(create_sql)

def insert_gtfs_table_rows(zipstream, table_name, conn):
	# get a list of columns in this table from the create_table schema.
	# We check the CSV column headers against this list later to check for non-existant fields
	column_names_sql = f"SELECT name FROM pragma_table_info('{table_name}')"
	expected_columns = [row[0] for row in conn.execute(column_names_sql).fetchall()]

	# create the csv reader
	reader = csv.reader(zipstream)

	# get the comma-separated column names from the csv file (first line)
	# and trim excess whitespace surrounding each column
	original_columns = next(reader)
	original_columns = [re.sub(r'\W+', '', e) for e in original_columns]

	# make another copy of the original column names
	csv_columns = original_columns.copy()

	# verify the csv column names match column names from the CREATE TABLE statement
	# remove any invalid columns from csv_columns
	for column in csv_columns:
		if not column in expected_columns:
			print(f"WARNING: Table '{table_name}' contains invalid column '{column}', skipping import for this column")
			csv_columns.remove(column)

	# make a list of valid CSV indices
	valid_indices = [original_columns.index(c) for c in csv_columns]

	# build the correct sql insert string for this table
	value_template = ("?," * len(csv_columns)).rstrip(",")
	valid_csv_columns = ",".join(csv_columns)
	sql_insert = f"INSERT INTO {table_name} ({valid_csv_columns}) VALUES ({value_template})"

	# for every line in csv file insert it as a row into the database,
	# excluding columns that do not exist in the table schema
	for csv_line in reader:
		valid_csv_line = [csv_line[i] for i in valid_indices]
		conn.execute(sql_insert, valid_csv_line)

	# return number of rows from from CSV file
	return reader.line_num

def insert_rows_from_zip(zipfile, table_name, table_file, create_sql, conn):
	file_in_zip = zipfile.open(table_file, 'r')
	with io.TextIOWrapper(file_in_zip, encoding='UTF-8') as zipstream:
		# Insert rows from CSV file contained in the zipped archive
		return insert_gtfs_table_rows(zipstream, table_name, conn)

def main():
	# setup CLI arguments
	parser = argparse.ArgumentParser(description='Import a GTFS archive into a database')
	parser.add_argument('-i', metavar='gtfs.zip', required=True, dest='input_file', help='zipped GTFS archive file input')
	parser.add_argument('-o', metavar='output.db', required=True, dest='output_file', help='database file output')
	args = parser.parse_args()

	print('Establishing connection to', args.output_file)
	conn = db.connect(args.output_file)

	gtfs_zip = None
	try:
		print('Opening GTFS archive', args.input_file)
		gtfs_zip = zipfile.ZipFile(args.input_file)
	except zipfile.BadZipFile as e:
		print('Error opening gtfs archive:', e)
		return 1

	gtfs_table_pairs = [
		# required files are agency, stops, routes, trips, stop_times
		("agency", SQL_CREATE_AGENCY),
		("stops", SQL_CREATE_STOPS),
		("routes", SQL_CREATE_ROUTES),
		("trips", SQL_CREATE_TRIPS),
		("stop_times", SQL_CREATE_STOP_TIMES),
		# conditionally required are calendar, calendar_dates
		("calendar", SQL_CREATE_CALENDAR),
		("calendar_dates", SQL_CREATE_CALENDAR_DATES),
		# optional are fare_attributes, fare_rules, shapes, frequencies, transfers, pathways, levels, feed_info
		("fare_attributes", SQL_CREATE_FARE_ATTRIBUTES),
		("fare_rules", SQL_CREATE_FARE_RULES),
		("shapes", SQL_CREATE_SHAPES),
		("frequencies", SQL_CREATE_FREQUENCIES),
		("transfers", SQL_CREATE_TRANSFERS),
		("pathways", SQL_CREATE_PATHWAYS),
		("levels", SQL_CREATE_LEVELS),
		("feed_info", SQL_CREATE_FEED_INFO)
	]

	# Perform some basic validation of the zipfile
	archive_file_list = gtfs_zip.namelist()
	required_gtfs_files = ['agency.txt', 'stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt']
	valid_gtfs_files = [pair[0] + '.txt' for pair in gtfs_table_pairs]

	# Are the minmum-required files present? If not, abort
	for required_file in required_gtfs_files:
		if not required_file in archive_file_list:
			print('ERROR: This zip is missing a required GTFS file:', required_file)
			print('This is not a valid GTFS archive!')
			return 1

	for archive_file in archive_file_list:
		if not archive_file in valid_gtfs_files:
			print('WARNING: Unknown file in archive:', archive_file)

	# Create db tables and insert rows from archive
	for pair in gtfs_table_pairs:
		table_name = pair[0]
		table_file = table_name + '.txt'
		create_sql = pair[1]
		# create the table
		print('::', table_name)
		create_gtfs_table(table_name, create_sql, conn);
		# if the zip archive contains a valid gtfs file
		if table_file in archive_file_list:
			# insert rows from the zip CSV file
			rows_read = insert_rows_from_zip(gtfs_zip, table_name, table_file, create_sql, conn)
			print('Inserted', rows_read, 'rows into', table_name)

	print(':: creating indexes')
	for (index_name, crete_sql) in SQL_LIST_CREATE_INDEXES:
		create_index(index_name, crete_sql, conn)

	# Cleanup and exit
	gtfs_zip.close()
	conn.commit()
	conn.close()
	print('Done')
	return 0

if __name__ == '__main__':
	main()
