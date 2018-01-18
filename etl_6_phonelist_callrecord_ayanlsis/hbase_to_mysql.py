# encoding: utf-8
# author : wudake
import pymongo,json,urllib,pymysql,time,datetime
from multiprocessing import Pool
import happybase,random,os,string
from time_transformation import *
import pandas as pd

def check_data_cdr(rowkey):
    print "-------------------------------------------------"
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:yys_data_cdr_history_phtime')
    for key, record in table.scan(row_prefix=rowkey):
        print key,record['cf:returnData']
        my_temp = json.loads(record['cf:returnData'])
        tel_in_30 = []
        tel_in_90 = []
        tel_out_30 = []
        tel_out_90 = []
        for i in range(len(my_temp)):
            #print my_temp[i]['callType']
            if my_temp[i]['callType'] == u'被叫':
                #print unixtime_to_day(my_temp[i]['cTime'])
                if int(unixtime_to_day(my_temp[i]['cTime'])) > my_date(31):
                    tel_in_30.append(my_temp[i])
                if int(unixtime_to_day(my_temp[i]['cTime'])) > my_date(91):
                    tel_in_90.append(my_temp[i])
            if my_temp[i]['callType'] == u'主叫':
                if int(unixtime_to_day(my_temp[i]['cTime'])) > my_date(31):
                    tel_out_30.append(my_temp[i])
                if int(unixtime_to_day(my_temp[i]['cTime'])) > my_date(91):
                    tel_out_90.append(my_temp[i])

        print "tel_in_30:" , len(tel_in_30),tel_in_30
        print "tel_in_90:" , len(tel_in_90),tel_in_90
        print "tel_out_30:" , len(tel_out_30),tel_out_30
        print "tel_out_90:" , len(tel_out_90),tel_out_90


if __name__ == '__main__':
    check_data_cdr('15205850903_201712')