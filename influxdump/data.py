# -*- coding: utf-8 -*-
import json

from db import get_queries, get_measurements


def read_data(c, pattern=None):
    """Get data from the database, return an `influxdb.ResultSet`

    :param c: an influxdb client instance
    :type c: InfluxDBClient
    :param measurements: a list of measurements to query
    :type measurements: list
    """
    measurements = get_measurements(c, pattern)
    queries = get_queries(measurements)
    return query_data(c, queries)


def query_data(c, queries):
    data = []
    for q in queries:
        res = c.query(q.get_query())
        records = []
        for point in res.get_points():
            records.append(point)
        data.append({
            "meta": q.get_meta(),
            "records": records
        })

    return data


def dump_data(data):
    print(json.dumps(data))
