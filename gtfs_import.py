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
)
"""

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
)
"""

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

class GTFSObject:
	def __init__(self, name, create_sql):
		self.table_name = name
		self.file_name = name + ".txt"
		self.create_sql = create_sql

def create_gtfs_table(file, table_name, sql_create_schema, cursor):
	# Drop old and create new table
	cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
	cursor.execute(sql_create_schema)

	# create the csv reader
	reader = csv.reader(file)

	# build the correct sql insert string for this table
	schema = next(reader)
	column_schema = ",".join([re.sub(r'\W+', '', e) for e in schema])
	value_template = ("?," * len(schema)).rstrip(",")
	sql_insert = f"INSERT INTO {table_name} ({column_schema}) VALUES ({value_template})"

	# for every line in csv file, insert it into database
	for line in reader:
		cursor.execute(sql_insert, line)

	print(f"Inserted {reader.line_num} rows into table '{table_name}'")

def create_and_insert(zipfile, go, conn):
	file_in_zip = zipfile.open(go.file_name, 'r')
	with io.TextIOWrapper(file_in_zip, encoding='UTF-8') as f:
		create_gtfs_table(f, go.table_name, go.create_sql, conn)

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

	gtfs_objects = [
		# required files are agency, stops, routes, trips, stop_times
		GTFSObject("agency", SQL_CREATE_AGENCY),
		GTFSObject("stops", SQL_CREATE_STOPS),
		GTFSObject("routes", SQL_CREATE_ROUTES),
		GTFSObject("trips", SQL_CREATE_TRIPS),
		GTFSObject("stop_times", SQL_CREATE_STOP_TIMES),
		# conditionally required are calendar, calendar_dates
		GTFSObject("calendar", SQL_CREATE_CALENDAR),
		GTFSObject("calendar_dates", SQL_CREATE_CALENDAR_DATES),
		# optional are fare_attributes, fare_rules, shapes, frequencies, transfers, pathways, levels, feed_info
		GTFSObject("fare_attributes", SQL_CREATE_FARE_ATTRIBUTES),
		GTFSObject("fare_rules", SQL_CREATE_FARE_RULES),
		GTFSObject("shapes", SQL_CREATE_SHAPES),
		GTFSObject("frequencies", SQL_CREATE_FREQUENCIES),
		GTFSObject("transfers", SQL_CREATE_TRANSFERS),
		GTFSObject("pathways", SQL_CREATE_PATHWAYS),
		GTFSObject("levels", SQL_CREATE_LEVELS),
		GTFSObject("feed_info", SQL_CREATE_FEED_INFO)
	]

	# Perform some basic validation of the zipfile
	archive_files = gtfs_zip.namelist()
	required_gtfs_files = ['agency.txt', 'stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt']
	valid_gtfs_files = [go.file_name for go in gtfs_objects]

	# Are the minmum-required files present? If not, abort
	for required_file in required_gtfs_files:
		if not required_file in archive_files:
			print('ERROR: This zip is missing a required GTFS file:', required_file)
			print('This is not a valid GTFS archive!')
			return 1

	# Are any extra files present? If not, print warning
	for archive_file in archive_files:
		if not archive_file in valid_gtfs_files:
			print('WARNING: Unknown file in archive:', archive_file)

	# Create a list of files to import
	files_to_import = [af for af in archive_files if af in valid_gtfs_files]

	# List the files to be imported
	print('Importing files:')
	for archive_file in files_to_import:
		print('\t', archive_file)

	# Create db tables and insert rows from archive
	for go in gtfs_objects:
		if go.file_name in files_to_import:
			create_and_insert(gtfs_zip, go, conn)
	
	# Cleanup and exit
	gtfs_zip.close()
	conn.commit()
	conn.close()
	print('Done')
	return 0

if __name__ == '__main__':
	main()
