# coding=utf-8
"""
a set database using file exchange
"""

from functools import partial
import configparser

configure = "byr.conf"
CONF = configparser.ConfigParser()
CONF.read(configure)

from os import listdir,sep
from pickle import load, dump


def with_database(dbfile=CONF.get("filepath", "joblist_db")):
    # todo: may be some problem with dbfile : should be updated later
    def call_func(fn):
        def inner_call(*args, **kwargs):
            # assert dbfile in the current basedir, this is a bug
            # that you can not set the dbfile to another path
            if dbfile not in listdir("."):
                with open(dbfile, "wb") as f:
                    empty = set()
                    dump(empty, f)
                    print("db is now established")
            with open(dbfile, "rb") as f:
                datatable = load(f)
            datatable, response = fn(datatable, *args, **kwargs)
            # always overwrite
            with open(dbfile, "wb") as f:
                dump(datatable, f)
            # now return
            return response

        return inner_call

    return call_func


@with_database()
def scan_database(datatable):
    return datatable, datatable


@with_database()
def insert_one(datatable, record):
    datatable.add(record)
    return datatable, "success"


@with_database()
def insert_batch(datatable, records):
    records = set(records)
    datatable = datatable.union(records)
    return datatable, "success"


@with_database()
def empty_database(datatable):
    datatable = set()
    return datatable, None


@with_database()
def drop_record(datatable, record):
    datatable = datatable - {record}
    return datatable, "success"


@with_database()
def drop_records(datatable, records):
    datatable = datatable - set(records)
    return datatable, "success"


@with_database()
def is_in_database(datatable, record):
    answer = 0
    if record in datatable:
        answer = 1
    return datatable, answer


if __name__ == '__main__':
    # insert_batch([1,2,3,"9","60"])
    print(scan_database())
    print(is_in_database(2))
    print(is_in_database(9))
