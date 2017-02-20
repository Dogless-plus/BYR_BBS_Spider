# coding=utf-8
# 论坛十大

from byr_util import get_page, load_mail_list
import re
from bs4 import BeautifulSoup
import pandas as pd
from mailbox import load_html_template, send_mail
from datetime import datetime
from time import sleep
import schedule


def get_topten(trylimit=3):
    """
    :return: topten text dataframe
    """
    df = pd.DataFrame()
    while trylimit:
        try:
            text = get_page("https://bbs.byr.cn/default?_uid=awsxsa")
            soup = BeautifulSoup(text, "lxml")
            topten_text = str(soup.find("li", id="topten"))
            pattern = re.compile(r'title=\"(.*?)\((.*?)\)\"\>.*?href=\"(.*?\d+)\"\>', re.UNICODE)
            topten = re.findall(pattern, topten_text)
            df = pd.DataFrame(topten, columns=["title", "hot", "address"], dtype="str")
            break
        except:
            df = pd.DataFrame()
            trylimit -= 1
    return df


def send_topten(contacts, df):
    # send email
    if df.shape[0] == 0:
        print("no such div : top ten")
        return
    html = load_html_template()
    row = "<tr><td>{0}</td><td>{1}</td> <td>{2}</td></tr>"
    trs = []
    trs.append(row.format("#", "题目", "热度"))
    for i, line in enumerate(df.itertuples()):
        tmp_row = row.format(str(i),
                             r'<a href="https://bbs.byr.cn/{0}">{1}<a>'.format(line[3], line[1]),
                             str(line[2]))
        trs.append(tmp_row)
    inner_trs = " ".join(trs)
    html = html.format(inner_trs)
    send_mail(to=contacts,
              subject='[皮皮虾] 今日十大 [%s]' % (datetime.now().strftime("%Y-%m-%d %H:%M")),
              contents=html,
              attachments=None)


def with_heartbeat_30s(fn):
    # timedtask wrapper
    def call_func(*args):
        HEARTBEAT = 30
        loop = 0
        fn(*args)
        schedule.every(HEARTBEAT).seconds.do(fn, *args)
        while 1:
            print("#" * 15, "loop:%s" % loop, "#" * 15)
            schedule.run_pending()
            sleep(HEARTBEAT)
            loop += 1

    return call_func


def with_heartbeat_1d(fn):
    # timedtask wrapper
    def call_func(*args):
        HEARTBEAT = 30
        loop = 0
        fn(*args)
        schedule.every().day.at("10:30").do(fn, *args)
        while 1:
            print("#" * 15, "loop:%s" % loop, "#" * 15)
            schedule.run_pending()
            sleep(HEARTBEAT)
            loop += 1

    return call_func


def run_batch():
    mail_list = load_mail_list()
    df = get_topten()
    send_topten(mail_list, df)
    print("sent")


def demo1():
    with_heartbeat_30s(run_batch)()


if __name__ == '__main__':
    demo1()
