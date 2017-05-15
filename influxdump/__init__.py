# -*- coding: utf-8 -*-
import argparse
import getpass
import json

from data import dump_data, write_data, load_data
from db import get_client


__author__ = 'Stefan Berder <stefan@measureofquality.com>'
__contact__ = 'code+influxdump@measureofquality.com'
__version__ = "1.0.3"


def get_args():
    parser = argparse.ArgumentParser(description='influxDB data backup tool')
    parser.add_argument('-d', '--database', help='database', required=True,
            type=str)
    parser.add_argument('-F', '--folder', default=None,
            help="destination folder for fragmented dump, if this flag is not used then dump on stdoout")
    parser.add_argument('-H', '--host', help='server host',
            default="localhost", type=str)
    parser.add_argument('-i', '--input', default=None,
            help="data/metadata input file, will force action to 'load'")
    parser.add_argument('-L', '--legacy', action="store_true",
            help='influxdb legacy client (<=0.8)')
    parser.add_argument('-m', '--measurements', help='measurement pattern')
    parser.add_argument('-n', '--dry-run', help='do mot really do anything', action="store_true")
    parser.add_argument('-p', '--port', help='server port', default=8086,
            type=int)
    parser.add_argument('-u', '--user', help='username', default='', type=str)
    parser.add_argument('-v', '--verbose', help='make the script verbose', action="store_true")
    parser.add_argument('-w', '--password', help='password', default='',
            type=str)
    parser.add_argument('-W', '--pwdprompt', help='password prompt',
            action="store_true")
    parser.add_argument('action', metavar="action", nargs="?", default='dump',
            help="action, can be 'dump' or 'load', default to 'dump'",
            choices=["load", "dump"])
    args = parser.parse_args()

    if args.pwdprompt is True:
        pwd = getpass.getpass()
    else:
        pwd = args.password

    return {
        "db": args.database,
        "folder": args.folder,
        "host": args.host,
        "input": args.input,
        "legacy": args.legacy,
        "measurements": args.measurements,
        "dryrun": args.dry_run,
        "port": args.port,
        "user": args.user,
        "verbose": args.verbose,
        "pwd": pwd,
        "action": args.action,
    }


def dump(args, client):
    dump_data(client, args["measurements"], args["folder"],
              dryrun=args["dryrun"], verbose=args["verbose"])


def load(args, client):
    data = load_data(args["input"])
    return write_data(client, data)


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
    main()
