gopartman
-----------

This package is designed to manage Postgres partitions and borrows (as in, copies and makes best efforts to keep up to date with) SQL from the wonderful 
[pg_partman](https://github.com/keithf4/pg_partman) extension for Postgres. Only, this package is not installing an extension into the Postgres database. 
In order to do this, the SQL from pg_partman was slightly modified.

This package is meant to build a binary which can be used from the command line or daemonized to run and constantly monitor and manage your Postgres partitions 
given instructions by you. It is configurable. It can also be brought into another package for use there as well and need not run by itself. Just keep 
in mind that there are regular tasks that need to be performed to manage your Postgres partition tables.
