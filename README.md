gopartman
-----------

This package is designed to manage Postgres partitions and borrows (as in, copies and makes best efforts to keep up to date with) SQL from the wonderful 
[pg_partman](https://github.com/keithf4/pg_partman) extension for Postgres. Basically, wraps it. This package is not installing an "extension" into the Postgres database, 
but it does put all of the functions into a "partman" schema to use. This makes it compatible with hosted Postgres services like Amazon RDS. In order to do this, the SQL 
from pg_partman was [slightly modified](http://www.databasesoup.com/2014/12/loading-pgpartman-on-rds-or-heroku.html).

This package wraps [sqlx](https://github.com/jmoiron/sqlx), which is a great Go package for SQL. So you have full access to its functions.    

pg_partman has many functions and you still have full access to them. You should reference the [pg_partman manual](https://github.com/keithf4/pg_partman/blob/master/doc/pg_partman.md) for 
andditional information and help.    

This package is meant to build a binary which can be used from the command line or daemonized (with optional RESTful API) to run and constantly monitor and manage your Postgres 
partitions. It can also be brought into another package for use there as well and need not run by itself. Just keep in mind that there are regular tasks that need to be performed 
to manage your Postgres partition tables. This package will do this maintenance on scheduled tasks which is why you might want to run it by itself.

### Features

* Command line interface for managing partitions (configured in YAML or added via RESTful API)    
* YAML configuration based management of partitions    
* RESTful interface for managing partitions    
* ~~RESTful API for monitoring (with SVG rendering)~~ (to come)    
* Monitoring of partition usage and database health    
