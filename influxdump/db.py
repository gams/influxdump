# -*- coding: utf-8 -*-
import re


class InfluxDBClient(object):
    def __init__(self,
            host,
            port,
            user,
            pwd,
            db,
    ):
        import influxdb

        self._client = influxdb.InfluxDBClient(
                host=host,
                port=port,
                username=user,
                password=pwd,
                database=db,
        )

    def query(self, *args, **kwargs):
        return self._client.query(*args, **kwargs)

    def show_measurements(self):
        return self._client.query("SHOW MEASUREMENTS")

    def get_points(self, res):
        for point in res.get_points():
            yield point

    def get_measurements(self, pattern=None):
        """List measurement from the database that are matching a pattern, returns
        a list of measurements.
        The pattern will be passed to `re.search()`.

        :param pattern: a pattern to match measurements
        :tuype pattern: string
        """
        res = self.show_measurements()
        measurements = []
        for point in self.get_points(res):
            measurement = point["name"]
            if pattern is None:
                measurements.append(measurement)
            else:
                if re.search(pattern, measurement):
                    measurements.append(measurement)

        return measurements

    def write_points(self, *args, **kwargs):
        return self._client.write_points(*args, **kwargs)


class InfluxDB08Client(InfluxDBClient):
    def __init__(self, host, port, user, pwd, db):
        import influxdb.influxdb08 as influxdb

        self._client = influxdb.InfluxDBClient(
                host=host,
                port=port,
                username=user,
                password=pwd,
                database=db,
        )

    def show_measurements(self):
        return self._client.query("LIST SERIES")

    def get_points(self, res):
        for point in res[0]["points"]:
            _point = {}
            for column in res[0]["columns"]:
                idx = res[0]["columns"].index(column)
                _point[column] = point[idx]
            yield _point


def get_client(host, port, user, pwd, db, legacy=False):
    """Return a configured influxdb client"""
    if legacy:
        return InfluxDB08Client(
                host=host,
                port=port,
                user=user,
                pwd=pwd,
                db=db,
        )
    else:
        return InfluxDBClient(
                host=host,
                port=port,
                user=user,
                pwd=pwd,
                db=db,
        )


class Query(object):
    measurement = None
    q = "SELECT * FROM \"{measurement}\""
    m = "SHOW FIELD KEYS FROM \"{measurement}\""
    ctx = {}

    def __init__(self, measurement, q=None, ctx={}):
        self.measurement = measurement
        if q is not None:
            self.q = q
        if ctx['start'] != '' or ctx['end'] !='':
            self.q += " WHERE "
            if ctx['start'] != '':
                self.q += "time >= '{start}'"
            if ctx['start'] != '' and ctx['end'] != '':
                self.q += " AND "
            if ctx['end'] != '':
                self.q += "time <= '{end}'"
        self.ctx = ctx

    def __repr__(self):
        return self.get_query()

    def get_query(self, ctx=None):
        _ctx = {"measurement": self.measurement}
        if ctx is not None:
            _ctx.update(ctx)
        if self.ctx is not None:
            _ctx.update(self.ctx)
        return self.q.format(**_ctx)

    def get_meta_query(self):
        _ctx = {"measurement": self.measurement}
        return self.m.format(**_ctx)

    def get_meta(self):
        return {
            "measurement": self.measurement,
            "query": self.q,
            "context": self.ctx,
        }


def get_queries(measurements, start='', end=''):
    queries = []
    for m in measurements:
        q = Query(m, ctx={'start': start, 'end': end})
        queries.append(q)

    return queries


def cast_value(name, value, cast={}):
    if name in cast:
        type = __builtins__[cast[name]]
        return type(value)
    elif '*' in cast:
        type = __builtins__[cast['*']]
        return type(value)
    return value


def data_to_points(measurement, records, typecast=False, cast={}):
    points = []
    for record in records:
        ts = record.pop("time")
        fields = {}
        for name, value in record.items():
            if value is not None:
                fields[name] = cast_value(name, value, cast)
        if fields:
            points.append({
                "measurement": measurement,
                "time": ts,
                "fields": fields,
            })

    return points
