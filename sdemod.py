import schedule
import time

def with_heartbeat(fn):
    def call_func(*args):
        loop=0
        schedule.every(5).seconds.do(fn,*args)
        while 1:
            print("#"*15,"loop:%s"%loop,"#"*15)
            schedule.run_pending()
            time.sleep(5)
            loop+=1
    return call_func

@with_heartbeat
def job():
    print("I'm working...")

if __name__ == '__main__':
    job()
