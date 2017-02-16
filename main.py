# coding=utf-8
# python3.5.2


from time import sleep
from datetime import datetime
import requests
import re
import numpy as np
import pandas as pd

pd.set_option('display.width', 320)
pd.set_option('display.max_colwidth', -1)
pd.set_option('float_format', '{:20,.4f}'.format)
import configparser
from file_set_db import scan_database, insert_batch, empty_database
from mailbox import send_job_list

configure = "byr.conf"
CONF = configparser.ConfigParser()
CONF.read(configure)
INSTANCE = {"username": CONF.get("userinfo", "username"),
            "password": CONF.get("userinfo", "password")}

BYR_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'https://bbs.byr.cn/'}


def get_cookie():
    """
    byr bbs login cookie
    :return: cookie(dict)
    """
    byr_login_url = r"https://bbs.byr.cn/user/ajax_login.json"
    byr_login_data = {"id": INSTANCE["username"],
                      "passwd": INSTANCE["password"],
                      "CookieDate": "2"}
    login = requests.post(byr_login_url, data=byr_login_data, headers=BYR_HEADER)
    # save cookie for later use
    byr_cookie_dict = requests.utils.dict_from_cookiejar(login.cookies)
    # print(byr_cookie_dict)
    return byr_cookie_dict


def with_log(fn, soft=True):
    """
    log wrapper
    :return: decorated with logger
    """

    def call_func(*args, **kwargs):
        try:
            response = fn(*args, **kwargs)
            print(
                "[OK] step:{0} ({1})  @{2}".format(fn.__name__, [str(ai) for ai in args[:5]], datetime.now().__str__()))
            return response
        except:
            print("[ERROR] step:{0} ({1})  @{2}".format(fn.__name__, [str(ai) for ai in args[:5]],
                                                        datetime.now().__str__()))
            if soft:
                pass
            else:
                raise

    return call_func


def with_byr(fn):
    """
    byr login context wrapper
    :param fn: method to be decorated
    :return: decorated method with session
    """

    def call_func(*args, **kwargs):
        session = requests.Session()
        byr_cookie = with_log(get_cookie)()
        requests.utils.add_dict_to_cookiejar(session.cookies, byr_cookie)
        session.headers = BYR_HEADER
        response = fn(session, *args, **kwargs)
        session.close()
        return response

    return call_func


@with_byr
def get_page(session, url, **kwargs):
    """
    get one page
    """
    html = session.get(url, **kwargs).text
    return html


@with_byr
def get_parttimejob_list(session, page_start=1, page_end=5, step_time=3, day_last=2):
    """
    :return: jobs info dataframe: columns("job_id","job_type","company","job_name","start_time")
    """

    def parse_one_list(page_idx):
        # get and parse one job list page
        job_url = r"https://bbs.byr.cn/board/ParttimeJob/"
        params = {"_uid": INSTANCE["username"], "p": page_idx}
        html = session.get(job_url, params=params).text
        html = html.replace("&amp;", "")
        pattern = re.compile(r'href\=\"\/article\/ParttimeJob\/(\d+)\"\>【(.*?)】.*?【(.*?)】(.*?)\<\/a\>.*?'
                             r'\<\/td\>\<td.*?\>(\d+\-\d+\-\d+)\<\/td\>',
                             re.UNICODE)
        jobs = re.findall(pattern, str(html))
        # print(page_idx,jobs)
        return jobs

    job_list = []
    # parse newest pages and then sum up
    for page_i in range(page_start, page_end + 1):
        trylimit = 3
        while 1:
            try:
                jobs = with_log(parse_one_list)(page_i)
                job_list.extend(jobs)
                sleep(step_time)
                break
            except:
                print("we should wait")
                sleep(100)
                trylimit -= 1
                if not trylimit:
                    print("unable to fetch this page %s" % page_i)
                    break

    # filter exists ones
    history_jobs = scan_database()
    # print(history_jobs)
    new_job_list = [item for item in job_list if item[0] not in history_jobs]
    if not new_job_list:
        return pd.DataFrame({})
    # append new ones to joblist database
    insert_batch([item[0] for item in new_job_list])

    job_df = pd.DataFrame(new_job_list,
                          columns=["job_id", "job_type", "company", "job_name", "start_time"],
                          dtype="str")
    # only use newest jobs
    job_df = job_df.sort_values(["start_time"], ascending=False)
    now = datetime.now()

    def filter_day(day):
        day2 = datetime.strptime(day, '%Y-%m-%d')
        last = abs(int((day2 - now).days))
        if last > day_last:
            return np.nan
        else:
            return day

    job_df["start_time"] = job_df["start_time"].apply(filter_day)
    job_df = job_df.dropna(axis=0)
    job_df["job_link"] = job_df["job_id"].apply(lambda id: "https://bbs.byr.cn/article/ParttimeJob/{0}".format(id))
    job_df.to_csv("jobs.csv", encoding="utf-8", index=False)

    return job_df


def load_mail_list(listfile=CONF.get("client","mail_list_db")):
    df=pd.read_csv(listfile,
                   encoding="utf-8",
                   names=["address"],
                   header=None,
                   dtype="str")
    mail_list=list(df["address"])   # do not use tuple here since yagmail's converter with bug
    return mail_list

def run_batch():
    # automatic job
    job_list = get_parttimejob_list()
    if job_list.shape[0] == 0:
        print(" no new jobs")
    else:
        print("%s new job" % job_list.shape[0])
        mail_list = load_mail_list()
        send_job_list(mail_list, job_list)
        print("sent")


# todo:new database api,proxypool,email_list, specific info, user-defined filter,timeclock

if __name__ == '__main__':
    empty_database()  # todo: remove when deploy : as only for test
    run_batch()


