######################################
influxdump - InfluxDB data backup tool
######################################

The influxdump utility performs data dumps, producing json files that can then
be loaded back into a database.
The major difference with `influxd backup
<https://docs.influxdata.com/influxdb/v1.1/administration/backup_and_restore/>`_
command is that `influxdump` is creating a data snapshot with a lot of
flexibility on how to load it back in the database.

Usage
=====

Dump all data from a database::

    $ influxdump -u jdoe -W -d database > data_dump.json

Dump data from measurements containing the string 'node' in chunk files of
50,000 records::

    $ influxdump -u jdoe -W -d database -f _dump -c 50000 -m "node"

Dump data from measurements starting with the string 'node' in chunk files of
10,000 records (default) between 1st January 2019 and 31st March 2019 in French
timezone::

    $ influxdump -u jdoe -W -d database -f _dump -m "^node.*" --start "2019-01-01T00:00:00+01:00" --end "2019-03-31T23:59:59+01:00"

Load data from a dump folder::

    $ influxdump -u jdoe -W -d database -f _dump


Install
=======

.. code:: sh

    $ pip install influxdump

Packaging
=========

Create packages:

.. code:: sh

    $ python setup.py sdist bdist_wheel

Push package:

.. code:: sh

    $ twine upload dist/*
    $ twine upload -r pypi dist/*


License
=======

This software is licensed under the Apache License 2.0. See the LICENSE file in the top distribution directory for the full license text.
