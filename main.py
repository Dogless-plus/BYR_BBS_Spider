# coding=utf-8
# python 3.5.2

from job_list import run_batch as run_batch_job
from file_set_db import empty_database

def demo1(deploy=False):
    if not deploy:
        empty_database()
    run_batch_job()


if __name__ == '__main__':
    demo1()
    # demo1(True)
