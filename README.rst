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

Dump data matching a pattern in chunk files of 50,000 records::

    $ influxdump -u jdoe -W -d database -f _dump -c 50000 -m "node*"

Load data from a dump folder::

    $ influxdump -u jdoe -W -d database -f _dump


Install
=======

.. code-block:: sh

    $ pip install influxdump

Packaging
=========

Create packages:

.. code-block:: sh

    $ python setup.py sdist bdist_wheel

Push package:

.. code-block:: sh

    $ twine upload dist/*
    $ twine upload -r pypi dist/*


License
=======

This software is licensed under the Apache License 2.0. See the LICENSE file in the top distribution directory for the full license text.
