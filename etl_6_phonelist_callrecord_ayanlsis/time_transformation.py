# encoding: utf-8
# author : wudake
import time,datetime

def unixtime_to_time(myunix_time):
    my_time = float(str(myunix_time)[:10])
    #print my_time
    #1465920361000
    b = time.localtime(my_time)
    target_time = time.strftime("%Y%m", b)
    #print target_time
    return target_time

def time_to_unixtime():
    endTimeStr = "2015-11-28 00:00:00"
    formatStr = "%Y-%m-%d %H:%M:%S"
    tmObject = time.strptime(endTimeStr, formatStr)
    tmStamp = time.mktime(tmObject)
    print tmStamp

def unixtime_to_day(myunix_time):
    my_time = float(str(myunix_time)[:10])
    #print my_time
    #1465920361000
    b = time.localtime(my_time)
    target_time = time.strftime("%Y%m%d", b)
    #print target_time
    return target_time

def my_date(before_days):
    the_day =  datetime.date.today() - datetime.timedelta(days=before_days)
    result = the_day.strftime("%Y%m%d")
    #print type(result)
    return int(result)

if __name__ == '__main__':
    #unixtime_to_time(1465920361000)
    time_to_unixtime()
    my_date(30)