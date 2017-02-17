# coding=utf-8

import re
import numpy as np
import pandas as pd
from time import sleep
from datetime import datetime
from byr_util import INSTANCE, with_byr, with_log, with_heartbeat, load_mail_list
from file_set_db import scan_database, insert_batch,empty_database
from mailbox import load_html_template, send_mail


@with_byr
def get_parttimejob_list(session, page_start=1, page_end=3, step_time=3, day_last=2):
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


def send_job_list(contacts, df):
    # df :job_id,job_type,company,job_name,start_time,job_link
    html = load_html_template()
    row = "<tr><td>{0}</td><td>{1}</td> <td>{2}</td><td>{3}</td><td>{4}</td></tr>"
    trs = []
    trs.append(row.format("#", "类型", "公司", "发布时间", "具体工作"))
    for i, line in enumerate(df.itertuples()):
        tmp_row = row.format(str(i), line[2], line[3], line[5], r'<a href="{0}">{1}<a>'.format(line[6], line[4]))
        trs.append(tmp_row)
    inner_trs = " ".join(trs)

    # inner_trs="<tr><td>there should be some data</td></tr>"   #default

    html = html.format(inner_trs)
    # print(html)
    n_job = df.shape[0]
    send_mail(to=contacts,
              subject='[皮皮虾] 说有 %s 个新工作 [%s]' % (str(n_job),
                                                 datetime.now().strftime("%Y-%m-%d %H:%M")),
              contents=html,
              attachments=None)


@with_heartbeat
def run_batch():
    # timed job

    # empty_database()   # todo:to remove : only for test
    try:
        job_list = get_parttimejob_list()
        if job_list.shape[0] == 0:
            print(" no new jobs")
        else:
            print("%s new job" % job_list.shape[0])
            mail_list = load_mail_list()
            send_job_list(mail_list, job_list)
            print("sent")
    except:
        print("[ERROR] running batch", datetime.now().__str__())
        pass


if __name__ == '__main__':
    run_batch()
