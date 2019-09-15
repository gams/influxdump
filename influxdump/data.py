# -*- coding: utf-8 -*-
from datetime import datetime
import json
import os
import os.path
import sys

from db import get_queries, data_to_points


def query_data(c, queries, chunk_size):
    """Generator querying the db and sending back data for each query as
    elements.
    """
    data = []
    for q in queries:
        res = c.query(q.get_query(),
                chunked=True,
                chunk_size=chunk_size)
        counter = 0
        for r in res:
            records = []
            counter += 1
            for point in c.get_points(r):
                records.append(point)

            yield (counter, {
                "meta": q.get_meta(),
                "records": records
            })


def dump_data(c, pattern=None, folder=None, dryrun=False, chunk_size=50000,
        start='', end='', verbose=False):
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
        for (counter, data) in query_data(c, queries, chunk_size):
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


def write_data(c, data):
    #for chunk in data:
    points = data_to_points(data["meta"]["measurement"],
                            data["records"])
    c.write_points(points, batch_size=10000)


def load_file(c, datafile, verbose=False):
    with open(datafile, 'r') as fh:
        data = json.load(fh)

        if verbose is True:
            sys.stdout.write(
                "> loading {} in {} ({} records) [{}]\n".format(
                datafile, data["meta"]["measurement"],
                len(data["records"]), datetime.now().isoformat()))

        write_data(c, data)


def load_folder(c, folder, verbose=False):
    for (dirpath, dirnames, filenames) in os.walk(folder):
        filenames.sort()
        for filename in filenames:
            if filename.endswith('.json'):
                datafile = os.path.join(dirpath, filename)

                with open(datafile, 'r') as fh:
                    data = json.load(fh)
                    if verbose is True:
                        sys.stdout.write(
                            "> loading {} in {} ({} records) [{}]\n".format(
                            datafile, data["meta"]["measurement"],
                            len(data["records"]), datetime.now().isoformat()))

                    write_data(c, data)
                    del data
