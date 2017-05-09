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

Dump data::

    $ influxdump -u root -p -d database > data_dump.json

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
