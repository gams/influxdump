# -*- coding: utf-8 -*-
from datetime import datetime
import json
import os.path
import sys

from db import get_queries, data_to_points


def query_data(c, queries):
    """Generator querying the db and sending back data for each query as
    elements.
    """
    data = []
    for q in queries:
        res = c.query(q.get_query())
        records = []
        for point in c.get_points(res):
            records.append(point)
        yield {
            "meta": q.get_meta(),
            "records": records
        }


def dump_data(c, pattern=None, folder=None, dryrun=False, verbose=False):
    """Get data from the database, return an `influxdb.ResultSet`

    :param c: an influxdb client instance
    :type c: InfluxDBClient
    :param measurements: a list of measurements to query
    :type measurements: list
    """
    measurements = c.get_measurements(pattern)
    if verbose is True or dryrun is True:
        sys.stdout.write("> {} measurements matched\n".format(
            len(measurements)))
    queries = get_queries(measurements)

    if dryrun is True:
        sys.stdout.write("> following measurements would be dumped:\n".format(
                len(measurements)))
        for m in measurements:
            sys.stdout.write("    {}\n".format(m))
    else:
        for data in query_data(c, queries):
            if folder is None:
                if verbose is True:
                    sys.stdout.write("> dumping {}\n".format(
                        data["meta"]["measurement"]))
                print(json.dumps(data))
            else:
                filename = data["meta"]["measurement"] + ".json"
                dumpfile = os.path.join(folder, filename)
                if verbose is True:
                    sys.stdout.write("> dumping {} to {} ({} records) [{}]\n".format(
                        data["meta"]["measurement"], filename,
                        len(data["records"]), datetime.now().isoformat()))
                with open(dumpfile, "w") as fd:
                    json.dump(data, fd)


def write_data(c, data):
    for chunk in data:
        points = data_to_points(chunk["meta"]["measurement"],
                                chunk["records"])
        c.write_points(points, batch_size=10000)


def load_data(datafile):
    with open(datafile, 'r') as fh:
        return json.load(fh)
