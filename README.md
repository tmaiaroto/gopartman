gopartman
-----------

This package is designed to manage Postgres partitions and borrows (as in, copies and makes best efforts to keep up to date with) SQL from the wonderful 
[pg_partman](https://github.com/keithf4/pg_partman) extension for Postgres. Only, this package is not installing an extension into the Postgres database. 
This makes it compatible with hosted Postgres services like Amazon RDS. In order to do this, the SQL from pg_partman was [slightly modified](http://www.databasesoup.com/2014/12/loading-pgpartman-on-rds-or-heroku.html).

This package extends [sqlx](https://github.com/jmoiron/sqlx), which is a great Go package for SQL. So you have full access to its functions.    

pg_partman has many functions and you still have full access to them. You should reference the [pg_partman manual](https://github.com/keithf4/pg_partman/blob/master/doc/pg_partman.md) for 
andditional information and help.    

This package is meant to build a binary which can be used from the command line or daemonized to run and constantly monitor and manage your Postgres partitions 
given instructions by you. It is configurable. It can also be brought into another package for use there as well and need not run by itself. Just keep 
in mind that there are regular tasks that need to be performed to manage your Postgres partition tables.

### Features

* Command line interface for managing partitions    
* YAML configuration based management of partitions    
* RESTful interface for managing partitions    
* RESTful API for monitoring (with SVG rendering)    
* Monitoring of partition usage and database health    

### Uses
There's a few possible use cases and reasons why you might want to use this package over the actual pg_partman extension. Especially if you're a Go developer.

* You can manage and monitor partitions on any server from your command line and crontab (much like the Python scripts in pg_partman)    
* You can bring this package into your Go application so it can manage partitions    
* You can manage multiple partitions on multiple databases with different rules from a YAML configuration (no need to set cron jobs for maintenance)    
* You can setup an API server using this package to manage partitions on databases via some sort of RESTful interface (again, no need to set cron jobs as this process runs forever)    

There of course, are also some drawbacks. Namely, this package needs to get manual updates when pg_partman upates. This means you'd need to re-build it and keep an eye out for updates. 
Fortunately, there shouldn't be too many updates.