# gtfs-rest

gtfs-rest is Python3/Flask application for serving [GTFS](https://developers.google.com/transit/gtfs/) data over a REST API.

The current API design is geared heavily toward the needs of [Web Transit Map](https://github.com/jnlon/web-transit-map), however a long-term goal is to
make gtfs-rest general stable interface for GTFS data that can hopefully be used as the basis for other web and mobile applications.

## Dependencies

- python3 >= 3.6
- python3-flask
- sqlite3

## Usage and Basic Setup

1. Retrieve a GTFS archive for your transit agency(s) from your city or a provider such as [transitfeeds.com](https://transitfeeds.com/)

2. Import the transit data into an sqlite database using `gtfs_import.py`
	- `./gtfs_import.py -i path/to/gtfs.zip -o my-city-gtfs.db`

3. Copy and edit the configuration file config.sample.py to config.py.
	- Update 'DATABASE' setting to reflect the path of your sqlite database

4. Execute `./run` if not running in production, otherwise [deploy the flask application as appropriate for your environment](https://flask.palletsprojects.com/en/1.1.x/deploying/)

5. Optionally, setup [Web Transit Map](https://github.com/jnlon/web-transit-map) which utilizes gtfs-rest

## API Documentation

The API is currently unstable so there is no solid documentation aside from the
source code. Please view `@app.route()` decorators in `gtfs_server.py` for the time being.
