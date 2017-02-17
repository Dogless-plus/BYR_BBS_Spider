# coding=utf-8
# python3.5.2


from time import sleep
from datetime import datetime
import requests
import pandas as pd
import schedule

pd.set_option('display.width', 320)
pd.set_option('display.max_colwidth', -1)
pd.set_option('float_format', '{:20,.4f}'.format)
import configparser

configure = "byr.conf"
CONF = configparser.ConfigParser()
CONF.read(configure)
INSTANCE = {"username": CONF.get("userinfo", "username"),
            "password": CONF.get("userinfo", "password")}
HEARTBEAT = CONF.getint("joblist", "heartbeat")

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
    # print(html)
    return html


def load_mail_list(listfile=CONF.get("client", "mail_list_db")):
    df = pd.read_csv(listfile,
                     encoding="utf-8",
                     names=["address"],
                     header=None,
                     dtype="str")
    mail_list = list(df["address"])  # do not use tuple here since yagmail's converter with bug
    return mail_list


def with_heartbeat(fn):
    # timedtask wrapper
    def call_func(*args):
        loop = 0
        fn(*args)
        schedule.every(HEARTBEAT).seconds.do(fn, *args)
        while 1:
            print("#" * 15, "loop:%s" % loop, "#" * 15)
            schedule.run_pending()
            sleep(HEARTBEAT)
            loop += 1

    return call_func


if __name__ == '__main__':
    pass
