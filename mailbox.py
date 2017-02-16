#coding=utf-8

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
    def call_func(*args,**kwargs):
        try:
            yag=yagmail.SMTP(user={INSTANCE["username"]:"[PPxia] BYR_BBS"},
                             password=INSTANCE["password"],
                             host=INSTANCE["host"],
                             port=INSTANCE["port"])
        except:
            raise ConnectionError("unable to connect to mail server")
        response=fn(yag,*args,**kwargs)
        yag.close()
        return response
    return call_func


@with_mail
def send_mail(yag,**kwargs):
        # send_mail(to="2295841729@qq.com",
        #       subject='[皮皮虾] new jobs',
        #       contents=html,
        #       attachments="README.TXT")
    success=1  # 1 ,success ; 0 ,someerror
    tos=kwargs["to"]
    if type(tos) != list:
        tos=[tos]
    for to in tos:
        try:
            yag.send(to=to,
                     subject=kwargs["subject"],
                     contents=kwargs["contents"],
                     attachments=kwargs["attachments"],
                     cc=INSTANCE["username"])

        except:
            print("[Warn] sending to %s"%to)
            success=0
    return success

def load_html_template(tempfile=CONF.get("filepath","joblist_template_html")):
    with open(tempfile,"rt") as f:
        lines=f.readlines()
        html =" ".join([line.strip() for line in lines])  #must be space; in case of \n
    return html

def send_job_list(contacts,df):
    # df :job_id,job_type,company,job_name,start_time,job_link
    html=load_html_template()
    row="<tr><td>{0}</td><td>{1}</td> <td>{2}</td><td>{3}</td><td>{4}</td></tr>"
    trs=[]
    trs.append(row.format("#","类型","公司","发布时间","具体工作"))
    for line in df.itertuples():
        tmp_row=row.format(line[0],line[2],line[3],line[5],r'<a href="{0}">{1}<a>'.format(line[6],line[4]))
        trs.append(tmp_row)
    inner_trs=" ".join(trs)

    # inner_trs="<tr><td>there should be some data</td></tr>"   #default

    html=html.format(inner_trs)
    # print(html)
    n_job=df.shape[0]
    send_mail(to=contacts,
              subject='[皮皮虾] 说有 %s 个新工作 [%s]'%(str(n_job),
                                               datetime.now().strftime("%Y-%m-%d %H:%M")),
              contents=html,
              attachments=None)


def demo1():
    html=load_html_template()
    inner_trs="<tr><td>there should be some data</td></tr>"  # default
    html=html.format(inner_trs)
    print(html)
    send_mail(to="2295841729@qq.com",
              subject='[皮皮虾] new jobs',
              contents=html,
              attachments="README.TXT")

def demo2():
    df=pd.read_csv(r"x_template/jobs.csv")
    send_job_list(["awsxsa@hotmail.com","xx@qq.com"],df)

if __name__ == '__main__':
    demo2()





