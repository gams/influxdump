# -*- coding: utf-8 -*-
from datetime import datetime
import json
import os
import os.path
import sys

from requests.exceptions import RequestException

from db import get_queries, data_to_points
from exception import TypecastError


def query_data(
        c,
        queries,
        chunk_size,
        typecast=False,
        cast={},
        retry=0
    ):
    """Generator querying the db and sending back data for each query as
    elements.
    """
    _r = 0
    for q in queries:
        while True:
            res = c.query(q.get_query(),
                    chunked=True,
                    chunk_size=chunk_size)
            counter = 0
            try:
                meta = get_meta(c, q, typecast, cast)
                for r in res:
                    records = []
                    counter += 1
                    for point in c.get_points(r):
                        records.append(point)

                    yield (counter, {
                        "meta": meta,
                        "records": records
                    })
                break
            except RequestException:
                if retry == 0:
                    raise
                _r += 1
                if _r > retry:
                    raise


def get_meta(c, q, typecast=False, cast={}):
    meta = q.get_meta()

    if typecast is True:
        if cast != {}:
            meta['types'] = cast
        else:
            _cast = {}
            res = c.query(q.get_meta_query())
            for p in res.get_points():
                if 'fieldType' not in p:
                    raise TypecastError("Field type cannot be guessed")
                if p['fieldType'] == 'string':
                    fieldtype = 'str'
                else:
                    fieldtype = p['fieldType']

                _cast[p['fieldKey']] = fieldtype
            meta['types'] = _cast

    return meta


def dump_data(
        c,
        pattern=None,
        folder=None,
        dryrun=False,
        chunk_size=50000,
        start='',
        end='',
        retry=0,
        typecast=False,
        cast={},
        verbose=False
    ):
    """Get data from the database, return an `influxdb.ResultSet`

    :param c: an influxdb client instance
    :type c: InfluxDBClient
    """
    measurements = c.get_measurements(pattern)
    if verbose is True or dryrun is True:
        sys.stdout.write("> {} measurements matched\n".format(
            len(measurements)))
    queries = get_queries(measurements, start=start, end=end)

    if dryrun is True:
        sys.stdout.write("> following measurements would be dumped:\n".format(
                len(measurements)))
        for m in measurements:
            sys.stdout.write("    {}\n".format(m))
    else:
        for (counter, data) in query_data(
                c,
                queries,
                chunk_size,
                typecast,
                cast,
                retry,
            ):
            if folder is None:
                if verbose is True:
                    sys.stdout.write("> dumping {}\n".format(
                        data["meta"]["measurement"]))
                print(json.dumps(data))
            else:
                bundle = os.path.join(folder,
                        data["meta"]["measurement"])
                if not os.path.exists(bundle):
                    os.makedirs(bundle)

                fragment = "{}-{:05d}.json".format(
                        data["meta"]["measurement"],
                        counter)
                dumpfile = os.path.join(bundle, fragment)
                data["meta"]["chunk_count"] = counter

                if verbose is True:
                    sys.stdout.write(
                        "> dumping {} (chunk {:05d}) to {} ({} records) [{}]\n".format(
                        data["meta"]["measurement"], counter, dumpfile,
                        len(data["records"]), datetime.now().isoformat()))

                with open(dumpfile, "w") as fd:
                    json.dump(data, fd)


def write_data(c, data, typecast=False, cast={}):
    points = data_to_points(data["meta"]["measurement"],
                            data["records"],
                            typecast,
                            cast)
    c.write_points(points, batch_size=10000)


def load_file(c, datafile, typecast=False, cast={}, verbose=False):
    with open(datafile, 'r') as fh:
        data = json.load(fh)

        if verbose is True:
            sys.stdout.write(
                "> loading {} in {} ({} records) [{}]\n".format(
                datafile, data["meta"]["measurement"],
                len(data["records"]), datetime.now().isoformat()))

        if typecast is True \
                and cast == {} \
                and "types" in data["meta"]:
            cast = data["meta"]["types"]

        write_data(c, data, typecast, cast)


def load_folder(c, folder, typecast=False, cast={}, verbose=False):
    for (dirpath, dirnames, filenames) in os.walk(folder):
        filenames.sort()
        for filename in filenames:
            if filename.endswith('.json'):
                datafile = os.path.join(dirpath, filename)
                load_file(c, datafile, typecast, cast, verbose)
