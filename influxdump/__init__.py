# -*- coding: utf-8 -*-
import argparse
import getpass
import json
import sys

from data import dump_data, load_file, load_folder
from db import get_client
from exception import TypecastError


CHUNKSIZE = 50000


def get_args():
    parser = argparse.ArgumentParser(description='influxDB data backup tool')
    parser.add_argument('-c', '--chunksize',
            help='query chunk size, default to {}'.format(CHUNKSIZE),
            type=int, default=CHUNKSIZE)
    parser.add_argument('-d', '--database', help='database', required=True,
            type=str)
    parser.add_argument('-e', '--end', default='', type=str,
            help="""
            Exclude all results after the specified timestamp (RFC3339 format).
            If used without -start, all data will be backed up starting from
            1970-01-01
            """)
    parser.add_argument('-F', '--folder', default=None,
            help="""
            destination folder for fragmented dump, if this flag is not used
            then dump on stdout
            """)
    parser.add_argument('-H', '--host', help='server host',
            default="localhost", type=str)
    parser.add_argument('-i', '--input', default=None,
            help="data/metadata input file, will force action to 'load'")
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
    parser.add_argument('-s', '--start', default='', type=str,
            help="""
            Include all points starting with the specified timestamp (RFC3339
            format)
            """)
    parser.add_argument('-t', '--typecast',
            help="""
            Enable casting field types based on file, meta or auto discovery
            if possible. When used with 'dump', will add casting infor in meta.
            When used with 'load', will try to find casting info. If casting is
            enabled but no casting info can be found, the program will exit.
            """, action="store_true")
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
    parser.add_argument('action', metavar="action", nargs="?", default='dump',
            help="""
            action, can be 'dump' or 'load', default to 'dump'. If action is
            'load', one input file (--input) or a folder with data to load has
            to be provided
            """, choices=["load", "dump"])
    args = parser.parse_args()

    if args.pwdprompt is True:
        pwd = getpass.getpass()
    else:
        pwd = args.password

    if args.end != "" and args.start == "":
        args.start = "1970-01-01T00:00:00Z"

    if args.castfile != '':
        with open(args.castfile, 'r') as fd:
            cast = json.load(fd)
    else:
        cast = {}

    if args.action == "load" \
            and args.input is None and args.folder is None:
        sys.stderr.write("Action is load, missing input file or folder\n\n")
        parser.print_help()
        sys.exit(1)


    return {
        "chunksize": args.chunksize,
        "db": args.database,
        "end": args.end,
        "folder": args.folder,
        "host": args.host,
        "input": args.input,
        "legacy": args.legacy,
        "measurements": args.measurements,
        "dryrun": args.dry_run,
        "port": args.port,
        "retry": args.retry,
        "start": args.start,
        "user": args.user,
        "verbose": args.verbose,
        "pwd": pwd,
        "action": args.action,
        "typecast": args.typecast,
        "cast": cast,
    }


def dump(args, client):
    dump_data(
        client,
        args["measurements"],
        args["folder"],
        dryrun=args["dryrun"],
        chunk_size=args["chunksize"],
        start=args["start"],
        end=args["end"],
        retry=args["retry"],
        typecast=args["typecast"],
        cast=args["cast"],
        verbose=args["verbose"]
    )


def load(args, client):
    if args["input"] is not None:
        load_file(
            client,
            args["input"],
            typecast=args["typecast"],
            cast=args["cast"],
            verbose=args["verbose"]
        )
    else:
        load_folder(
            client,
            args["folder"],
            typecast=args["typecast"],
            cast=args["cast"],
            verbose=args["verbose"]
        )


def main():
    args = get_args()
    client = get_client(
            host=args["host"],
            port=args["port"],
            user=args["user"],
            pwd=args["pwd"],
            db=args["db"],
            legacy=args["legacy"],
    )

    if args["action"] == "load" or args["input"] is not None:
        load(args, client)
    else:
        dump(args, client)


if __name__ == "__main__":
    try:
        main()
    except TypecastError as e:
        sys.stderr.write("""Error trying to guess field types for casting,
        influxdb < 1.0 did not provide key types when queried.
        """)
        sys.exit(1)
