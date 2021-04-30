# -*- coding: utf-8 -*-
"""Utility to recast dump files
"""
import argparse
import getpass
import json
import os
import re
import sys

from influxdump.data import get_meta
from influxdump.db import Query, get_client
from influxdump.exceptions import TypecastError


def get_args():
    parser = argparse.ArgumentParser(description='influxDB data dump recasting')
    parser.add_argument('-d', '--database', help='database', required=True,
            type=str)
    parser.add_argument('-H', '--host', help='server host',
            default="localhost", type=str)
    parser.add_argument('-L', '--legacy', action="store_true",
            help='influxdb legacy client (<=0.8)')
    parser.add_argument('-m', '--measurements', help='measurement pattern')
    parser.add_argument('-p', '--port', help='server port', default=8086,
            type=int)
    parser.add_argument('-r', '--retry', default=0, type=int,
            help="""
            Retry a dump query in case of problem, 0 to disable, defaults to 0
            """)
    parser.add_argument('-u', '--user', help='username', default='', type=str)
    parser.add_argument('-v', '--verbose', help='make the script verbose',
            action="store_true")
    parser.add_argument('-w', '--password', help='password', default='',
            type=str)
    parser.add_argument('-W', '--pwdprompt', help='password prompt',
            action="store_true")
    parser.add_argument('folder',
            help="""folder containing dump files""")
    args = parser.parse_args()

    if args.pwdprompt is True:
        pwd = getpass.getpass()
    else:
        pwd = args.password

    return {
        "db": args.database,
        "host": args.host,
        "legacy": args.legacy,
        "measurements": args.measurements,
        "port": args.port,
        "retry": args.retry,
        "user": args.user,
        "verbose": args.verbose,
        "pwd": pwd,
        "folder": args.folder,
    }


def main():
    args = get_args()
    verbose = args["verbose"]
    
    client = get_client(
            host=args["host"],
            port=args["port"],
            user=args["user"],
            pwd=args["pwd"],
            db=args["db"],
            legacy=args["legacy"],
    )

    if args["measurements"]:
        pattern = re.compile(args["measurements"])
    else:
        pattern = None

    if verbose is True:
        sys.stdout.write(
            "# Unknown measurement\n"
            "X Differing casting types\n"
            "* Database casting includes dump casting types\n"
            "= Same casting\n"
            "\n"
        )

    for entry in os.scandir(args["folder"]):
        if entry.is_dir():
            if pattern is not None and not pattern.search(entry.name):
                continue

            for filename in os.scandir(entry.path):
                if not filename.name.endswith('.json'):
                    continue

                if verbose is True:
                    sys.stdout.write(
                        "> test casting for {}\n".format(filename.name))

                with open(filename.path, 'r') as fd:
                    data = json.load(fd)

                q = Query(
                    data["meta"]["measurement"],
                    q=data["meta"]["query"],
                    ctx=data["meta"]["context"],
                )
                meta = get_meta(client, q, typecast=True)
                # Measurement does not exist in db
                if meta["types"] == {}:
                    sys.stdout.write(
                        "# {}\n".format(data["meta"]["measurement"])
                    )
                    continue

                # Measurement does not exist in db
                if meta["types"] == data["meta"]["types"]:
                    sys.stdout.write(
                        "= {}\n".format(data["meta"]["measurement"])
                    )
                else:
                    diff = False
                    for (k, v) in data["meta"]["types"].items():
                        if k in meta["types"] \
                                and v != meta["types"][k]:
                            diff = True
                            break

                    if diff is True:
                        sys.stdout.write(
                            "X {}\n".format(data["meta"]["measurement"])
                        )
                    else:
                        sys.stdout.write(
                            "* {}\n".format(data["meta"]["measurement"])
                        )

                    if verbose is True:
                        sys.stdout.write(
                            "dump: {}\n".format(data["meta"]["types"])
                        )
                        sys.stdout.write(
                            "db  : {}\n".format(meta["types"])
                        )


if __name__ == "__main__":
    try:
        main()
    except TypecastError as e:
        sys.stderr.write("""Error trying to get field types for casting,
        influxdb < 1.0 did not provide key types when queried.
        """)
        sys.exit(1)
