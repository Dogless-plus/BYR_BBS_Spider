# coding=utf-8

import yagmail
import pandas as pd
import configparser
from datetime import datetime

configure = "byr.conf"
CONF = configparser.ConfigParser()
CONF.read(configure)
INSTANCE = {"username": CONF.get("userinfo", "smtp_account"),
            "password": CONF.get("userinfo", "smtp_passcode"),
            "host": CONF.get("userinfo", "smtp_host"),
            "port": CONF.get("userinfo", "smtp_port")}


def with_mail(fn):
    def call_func(*args, **kwargs):
        try:
            yag = yagmail.SMTP(user={INSTANCE["username"]: "[PPxia] BYR_BBS"},
                               password=INSTANCE["password"],
                               host=INSTANCE["host"],
                               port=INSTANCE["port"])
        except:
            raise ConnectionError("unable to connect to mail server")
        response = fn(yag, *args, **kwargs)
        yag.close()
        return response

    return call_func


@with_mail
def send_mail(yag, **kwargs):
    # send_mail(to="2295841729@qq.com",
    #       subject='[皮皮虾] new jobs',
    #       contents=html,
    #       attachments="README.TXT")
    success = 1  # 1 ,success ; 0 ,someerror
    tos = kwargs["to"]
    if type(tos) != list:
        tos = [tos]
    for to in tos:
        try:
            yag.send(to=to,
                     subject=kwargs["subject"],
                     contents=kwargs["contents"],
                     attachments=kwargs["attachments"],
                     cc=INSTANCE["username"])

        except:
            print("[Warn] sending to %s" % to)
            success = 0
    return success


def load_html_template(tempfile=CONF.get("filepath", "joblist_template_html")):
    with open(tempfile, "rt") as f:
        lines = f.readlines()
        html = " ".join([line.strip() for line in lines])  # must be space; in case of \n
    return html


def demo1():
    html = load_html_template()
    inner_trs = "<tr><td>there should be some data</td></tr>"  # default
    html = html.format(inner_trs)
    print(html)
    send_mail(to="2295841729@qq.com",
              subject='[皮皮虾] new jobs',
              contents=html,
              attachments="README.TXT")


def demo2():
    df = pd.read_csv(r"x_template/jobs.csv")
    # send_job_list(["awsxsa@hotmail.com","xx@qq.com"],df)


if __name__ == '__main__':
    demo2()
