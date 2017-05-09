# -*- coding: utf-8 -*-
import json

from db import get_queries, data_to_points


def read_data(c, pattern=None):
    """Get data from the database, return an `influxdb.ResultSet`

    :param c: an influxdb client instance
    :type c: InfluxDBClient
    :param measurements: a list of measurements to query
    :type measurements: list
    """
    measurements = c.get_measurements(pattern)
    queries = get_queries(measurements)
    return query_data(c, queries)


def query_data(c, queries):
    data = []
    for q in queries:
        res = c.query(q.get_query())
        records = []
        for point in c.get_points(res):
            records.append(point)
        data.append({
            "meta": q.get_meta(),
            "records": records
        })

    return data


def dump_data(data):
    print(json.dumps(data))


def write_data(c, data):
    for chunk in data:
        points = data_to_points(chunk["meta"]["measurement"],
                                chunk["records"])
        c.write_points(points, batch_size=10000)


def load_data(datafile):
    with open(datafile, 'r') as fh:
        return json.load(fh)
