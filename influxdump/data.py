# -*- coding: utf-8 -*-
import json
import os.path

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


def dump_data(c, pattern=None, folder=None):
    """Get data from the database, return an `influxdb.ResultSet`

    :param c: an influxdb client instance
    :type c: InfluxDBClient
    :param measurements: a list of measurements to query
    :type measurements: list
    """
    measurements = c.get_measurements(pattern)
    queries = get_queries(measurements)
    for data in query_data(c, queries):
        if folder is None:
            print(json.dumps(data))
        else:
            dumpfile = os.path.join(folder, data["meta"]["measurement"] + ".json")
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
