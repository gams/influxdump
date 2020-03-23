#########
Changelog
#########

v1.0.4 (2020/03/23)
===================

This is the last version that will be tested against py2

* added a retry argument to the CLI for choppy connections
* added start and end time boundaries for incremental dumps
* added chunk dumps and found a bug in influxdb-python (see
  https://github.com/influxdata/influxdb-python/pull/753)
* added type casting for field values, auto type guessing or type casting
  through a cast file
