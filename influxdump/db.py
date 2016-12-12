# -*- coding: utf-8 -*-
import re

import influxdb


def get_client(host, port, user, pwd, db, legacy=False):
    """Return a configured influxdb client"""
    if legacy:
        return influxdb.influxdb08,InfluxDBClient(
                host=host,
                port=port,
                username=user,
                password=pwd,
                database=db,
        )
    else:
        return influxdb.InfluxDBClient(
                host=host,
                port=port,
                username=user,
                password=pwd,
                database=db,
        )


def get_measurements(c, pattern=None):
    """List measurement from the database that are matching a pattern, returns
    a list of measurements.
    The pattern will be passed to `re.search()`.

    :param c: an influxdb client instance
    :type c: InfluxDBClient
    :param pattern: a pattern to match measurements
    :tuype pattern: string
    """
    res = c.query("SHOW MEASUREMENTS")
    measurements = []
    for point in res.get_points():
        measurement = point["name"]
        if pattern is None:
            measurements.append(measurement)
        else:
            if re.search(pattern, measurement):
                measurements.append(measurement)

    return measurements


class Query(object):
    measurement = None
    q = "SELECT * FROM \"{measurement}\""
    ctx = None

    def __init__(self, measurement, q=None, ctx=None):
        self.measurement = measurement
        if q is not None:
            self.q = q
        self.ctx = ctx

    def __repr__(self):
        return self.get_query()

    def get_query(self, ctx=None):
        _ctx = {"measurement": self.measurement}
        if ctx is not None:
            _ct.update(ctx)
        if self.ctx is not None:
            _ctx.update(self.ctx)
        return self.q.format(**_ctx)

    def get_meta(self):
        return {
            "measurement": self.measurement,
            "query": self.q,
            "context": self.ctx,
        }


def get_queries(measurements, ctx=None):
    queries = []
    for m in measurements:
        q = Query(m)
        queries.append(q)

    return queries


def data_to_points(measurement, records):
    points = []
    for record in records:
        ts = record.pop("time")
        fields = {}
        for name, value in record.items():
            if value is not None:
                fields[name] = float(value)
        if fields:
            points.append({
                "measurement": measurement,
                "time": ts,
                "fields": fields,
            })

    return points
