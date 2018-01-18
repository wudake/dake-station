# encoding: utf-8
# author : wudake
# run command "hbase thrift -p 9090 start" on server 192.168.18.90 before run this python script
# hbase thrift -p 9090 start

import pymongo,json,urllib,pymysql,time,datetime
from multiprocessing import Pool
import happybase,random,os,string
from time_transformation import unixtime_to_time

def check_yys_data_basic_history_phtime():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:yys_data_basic_history_phtime')
    for key,record in table.scan(row_prefix=b'15104119515_201706'):
        print key,record
        for x,y in record.iteritems():
            print x,y
def check_source_table_basic():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:operator_callrecord_history_phtime')
    for key, record in table.scan(row_prefix=b'15104119515_1498758919'):
        if key != 'cf:returnData':
            print key,record
def check_yys_data_cdr_history_phtime():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:yys_data_cdr_history_phtime')
    for key,record in table.scan(row_prefix=b'18611643101_201601'):
        #print key,record
        for x,y in record.iteritems():
            print x,y
        print record['cf:returnData']
        abc = json.loads(record['cf:returnData'])
        print len(abc)

def check_source_table_cdr():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:operator_callrecord_history_phtime')
    f = open('temp_result.txt','w')
    result = []
    for key, record in table.scan(row_prefix=b'18611643101_1498717743'):
        print key,type(record)
        #print record['cf:returnData']
        my_record = json.loads(record['cf:returnData'])
        #print my_record['user']['telData']
        f.write(json.dumps(my_record['user']['telData']).encode('utf-8'))
        for i in range(len(my_record['user']['telData'])):
            #print my_record['user']['telData'][i]
            #print "--------------------------------"
            result.append(unixtime_to_time(my_record['user']['telData'][i]['cTime']))
            if unixtime_to_time(my_record['user']['telData'][i]['cTime']) == '201601':
                #print my_record['user']['telData'][i]
                print "------"
                for k, v in my_record['user']['telData'][i].iteritems():
                    print k,v
                print "------"
        #print len(my_record['user']['telData'])
    # uniq_result = list(set(result))
    # for i in uniq_result:
    #     print "18611643101_"+i
    # print result
    # print uniq_result
def check_yys_data_sms_history_phtime():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:yys_data_sms_history_phtime')
    for key,record in table.scan(row_prefix=b'15381417888_201605'):
        #print key,record
        for x,y in record.iteritems():
            print x,y
        print record['cf:returnData']
        abc = json.loads(record['cf:returnData'])
        print len(abc)

def check_source_table_sms():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:operator_callrecord_history_phtime')
    f = open('temp_result.txt','w')
    result = []
    for key, record in table.scan(row_prefix=b'15381417888_1498779113'):
        print key,type(record)
        #print record['cf:returnData']
        my_record = json.loads(record['cf:returnData'])
        #print my_record['user']
        f.write(json.dumps(my_record['user']['messageData']).encode('utf-8'))
        for i in range(len(my_record['user']['messageData'])):
            if unixtime_to_time(my_record['user']['messageData'][i]['sentTime']) == '201605':
                print "------"
                for k, v in my_record['user']['messageData'][i].iteritems():
                    print k, v
                print "------"


def check_yys_data_net_history_phtime():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:yys_data_net_history_phtime')
    for key,record in table.scan(row_prefix=b'15046218111_201509'):
        #print key,record
        for x,y in record.iteritems():
            print x,y
        print record['cf:returnData']
        abc = json.loads(record['cf:returnData'])
        print len(abc)

def check_source_table_yys():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:operator_callrecord_history_phtime')
    f = open('temp_result.txt','w')
    for key, record in table.scan(row_prefix=b'15046218111_1498748733'):
        print key,type(record)
        #print record['cf:returnData']
        my_record = json.loads(record['cf:returnData'])
        #print my_record['user']
        f.write(json.dumps(my_record['user']['flowDetail']).encode('utf-8'))
        for i in range(len(my_record['user']['flowDetail'])):
            if unixtime_to_time(my_record['user']['flowDetail'][i]['cTime']) == '201509':
                print "------"
                for k, v in my_record['user']['flowDetail'][i].iteritems():
                    print k, v
                print "------"

def check_yys_data_month_bill_history_phtime():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:yys_data_month_bill_history_phtime')
    for key,record in table.scan(row_prefix=b'13266669894_201602'):
        #print key,record
        for x,y in record.iteritems():
            print x,y
        print record['cf:returnData']
        abc = json.loads(record['cf:returnData'])
        print len(abc)

def check_source_table_month_bill():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:operator_callrecord_history_phtime')
    f = open('temp_result.txt','w')
    for key, record in table.scan(row_prefix=b'13266669894_1498736048'):
        print key,type(record)
        #print record['cf:returnData']
        my_record = json.loads(record['cf:returnData'])
        #print my_record['user']
        f.write(json.dumps(my_record['user']['billData']).encode('utf-8'))
        for i in range(len(my_record['user']['billData'])):
            #print my_record['user']['telData'][i]
            print "--------------------------------"
            for k,v in my_record['user']['billData'][i].iteritems():
                print k,v
        print len(my_record['user']['billData'])

def check_yys_data_net_bill_history_phtime():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:yys_data_net_bill_history_phtime')
    for key,record in table.scan(row_prefix=b'13507558709_201508'):
        #print key,record
        for x,y in record.iteritems():
            print x,y
        print record['cf:returnData']
        abc = json.loads(record['cf:returnData'])
        print len(abc)

def check_source_table_net_bill():
    connection = happybase.Connection('192.168.18.90', port=9090, autoconnect=False)
    connection.open()
    table = connection.table('crawler:operator_callrecord_history_phtime')
    f = open('temp_result.txt','w')
    for key, record in table.scan(row_prefix=b'13507558709_1498682495'):
        print key,type(record)
        #print record['cf:returnData']
        my_record = json.loads(record['cf:returnData'])
        #print my_record['user']
        f.write(json.dumps(my_record['user']['flowBill']).encode('utf-8'))
        for i in range(len(my_record['user']['flowBill'])):
            #print my_record['user']['telData'][i]
            print "--------------------------------"
            for k,v in my_record['user']['flowBill'][i].iteritems():
                print k,v
            print unixtime_to_time(my_record['user']['flowBill'][i]['time'])
        print len(my_record['user']['flowBill'])

if __name__ == '__main__':
    check_source_table_basic()
    print "----------------------------------------------------------------------------------------------------------------------------"
    check_yys_data_basic_history_phtime()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_source_table_cdr()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_yys_data_cdr_history_phtime()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_source_table_sms()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_yys_data_sms_history_phtime()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_source_table_yys()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_yys_data_net_history_phtime()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_source_table_month_bill()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_yys_data_month_bill_history_phtime()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_source_table_net_bill()
    print "----------------------------------------------------------------------------------------------------------------------------"
    #check_yys_data_net_bill_history_phtime()
    print "----------------------------------------------------------------------------------------------------------------------------"