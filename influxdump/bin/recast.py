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
    parser.add_argument('-d', '--database', help='database',
            type=str)
    parser.add_argument('-H', '--host', help='server host',
            default="localhost", type=str)
    parser.add_argument('-L', '--legacy', action="store_true",
            help='influxdb legacy client (<=0.8)')
    parser.add_argument('-m', '--measurements', help='measurement pattern')
    parser.add_argument('-n', '--dry-run', help='do not really do anything',
            action="store_true")
    parser.add_argument('-p', '--port', help='server port', default=8086,
            type=int)
    parser.add_argument('-r', '--retry', default=0, type=int,
            help="""
            Retry a dump query in case of problem, 0 to disable, defaults to 0
            """)
    parser.add_argument('--castfile',
            help="""
            File containing casting definitions, will supersede any other type
            cast definition
            """, type=str, default='')
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

    # if we're missing both castfile and database
    if args.castfile == '' \
            and args.database is None:
        sys.stderr.write("Casting source missing: provide either database or castfile\n\n")
        parser.print_help()
        sys.exit(1)

    # if we're getting both castfile and database
    if args.castfile != '' \
            and args.database is not None:
        sys.stderr.write("Casting souce conflict: provide either database or castfile (not both)\n\n")
        parser.print_help()
        sys.exit(1)

    if args.pwdprompt is True:
        pwd = getpass.getpass()
    else:
        pwd = args.password

    if args.castfile != '':
        with open(args.castfile, 'r') as fd:
            cast = json.load(fd)
    else:
        cast = {}

    return {
        "db": args.database,
        "host": args.host,
        "legacy": args.legacy,
        "measurements": args.measurements,
        "dryrun": args.dry_run,
        "port": args.port,
        "retry": args.retry,
        "user": args.user,
        "verbose": args.verbose,
        "pwd": pwd,
        "folder": args.folder,
        "cast": cast,
    }


def main():
    args = get_args()
    verbose = args["verbose"]
    dryrun = args["dryrun"]
    
    if args["db"] is None:
        client = None
    else:
        client = get_client(
                host=args["host"],
                port=args["port"],
                user=args["user"],
                pwd=args["pwd"],
                db=args["db"],
                legacy=args["legacy"],
        )

    cast = args["cast"]

    if args["measurements"]:
        pattern = re.compile(args["measurements"])
    else:
        pattern = None

    for entry in os.scandir(args["folder"]):
        if entry.is_dir():
            if pattern is not None \
                    and pattern.search(entry.name) is None:
                continue

            for filename in os.scandir(entry.path):
                if not filename.name.endswith('.json'):
                    continue

                if verbose is True:
                    sys.stdout.write(
                        "> recasting {}\n".format(filename.name))

                with open(filename.path, 'r') as fd:
                    data = json.load(fd)

                q = Query(
                    data["meta"]["measurement"],
                    q=data["meta"]["query"],
                    ctx=data["meta"]["context"],
                )
                meta = get_meta(client, q, typecast=True, cast=cast)
                # skip dumps we can't recast
                if meta["types"] == {}:
                    if verbose is True:
                        sys.stdout.write(
                            "  no casting change\n")
                    continue

                if verbose is True:
                    sys.stdout.write(
                        "  new casting: {}\n".format(meta["types"]))

                data["meta"]["types"] = meta["types"]

                if dryrun is False:
                    with open(filename.path, 'w') as fd:
                        json.dump(data, fd)


if __name__ == "__main__":
    try:
        main()
    except TypecastError as e:
        sys.stderr.write("""Error trying to guess field types for casting,
        influxdb < 1.0 did not provide key types when queried.
        """)
        sys.exit(1)
