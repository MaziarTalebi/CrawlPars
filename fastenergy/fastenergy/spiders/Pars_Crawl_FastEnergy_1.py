import scrapy
import random
import os
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import glob
import lxml.html as lh
from lxml import etree
import datetime
import datefinder
import w3lib.encoding
import MySQLdb
import hashlib
from scrapy.exceptions import CloseSpider
import re
import json
import html
import xml.sax.saxutils as saxutils
from fastenergy.items import FastenergyItem

class Pars_Crawl_fastenergy(scrapy.Spider):
    name = "fastenergy1"
    zipcode_failed = []
    zipcode_done = []
    ip_failed = []
    IPs = []
    Zipcodes = []
    def connection(self):
        db_conn=MySQLdb.connect(host="35.158.85.11",
                             port=3306,
                             user="mazi",
                             passwd="mazi2crawler",
                             db="crawler",
                             use_unicode=True,
                             charset='utf8')
        db_curs=db_conn.cursor()
        return db_curs,db_conn
    def Check_Generated_Url(self,zipcode,up,amount):
        print("let's check ::",zipcode,up,amount)
        db_curs,db_conn = self.connection()
        query = """SELECT generated_url FROM Generated_URL where zipcode='@@zipcode@@' and up='@@up@@' and amount='@@amount@@'"""
        query = query.replace("@@zipcode@@",zipcode)
        query = query.replace("@@up@@",up)
        query = query.replace("@@amount@@",amount)
        db_curs.execute(query)
        res = db_curs.fetchall()
        return res[0][0]

    def Load_IPs(self):
        db_curs,db_conn = self.connection()
        query = """SELECT ip_name FROM ips where ip_status='1'"""
        db_curs.execute(query)
        IPS_table = db_curs.fetchall()
        newlist=[]
        for ip in IPS_table:
            newlist.append(ip[0])
        return newlist
    def Load_Zipcodes(self):
        db_curs,db_conn = self.connection()
        query = """SELECT zip_zipcode FROM zipcode"""
        db_curs.execute(query)
        Zipcodes_table = db_curs.fetchall()
        return Zipcodes_table
    def BList_Request(self):
        f = open("listofurl.txt","r")
        mylist = f.readlines()
        f.close()
        return mylist
    def AList_Request(self):
        db_curs,db_conn = self.connection()
        query = """SELECT * FROM requests_python WHERE LENGTH(rep_zipcode)>0"""
        db_curs.execute(query)
        req = db_curs.fetchall()
        db_curs.close()
        db_conn.close()
        return req
    def List_Request(self,my_rep_id):
        my_rep_id = my_rep_id.replace(" ","")
        db_curs,db_conn = self.connection()
        query = """SELECT * FROM requests_python where rep_id in %s"""
        arg = []
        for a in my_rep_id.split(","):
            arg.append(a)
        totalarg = [arg]
        db_curs.execute(query,totalarg)
        req = db_curs.fetchall()
        db_curs.close()
        db_conn.close()
        return req
    def MD5_HASH(self,md5str):
        md5obj=hashlib.md5()
        md5obj.update(md5str)
        return md5obj.hexdigest()
    def table_time_setting(self,start_str,end_str):
        start_list = start_str.split(':')
        end_list = end_str.split(':')
        tmp = datetime.datetime.now()
        start_at_time = tmp.replace(hour=int(start_list[0]),minute=int(start_list[1]),second=int(start_list[2]),microsecond=0)
        end_at_time = tmp.replace(hour=int(end_list[0]),minute=int(end_list[1]),second=int(end_list[2]),microsecond=0)
        return start_at_time,end_at_time
    def __init__(self,arg_repid=None,*args, **kwargs):
        super(Pars_Crawl_fastenergy, self).__init__(*args, **kwargs)
        self.myspider_start_at = datetime.datetime.now()
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.crawl_list_jobs = []
        print(arg_repid)
        if arg_repid == "hourly" or arg_repid == "bigcrawl" or arg_repid == "test":
            req = self.AList_Request()
        elif "livecrawl" in arg_repid:
            newarg_repid = arg_repid.replace("livecrawl","")
            req = self.List_Request(newarg_repid)
        for r in req:
            start_at,end_at = self.table_time_setting(str(r[7]),str(r[8]))
            now = datetime.datetime.now()
            #now = now.replace(hour=7)
            # now = now.replace(day=4,hour=4)
            # start_at = start_at.replace(day=4)
            # end_at = end_at.replace(day=4)
            if arg_repid == "hourly":
                # whether hourly crawl or big crawl
                if now >= start_at and now <= end_at:
                    if r[6] == 1 and now.weekday() == r[9]:
                        # big crawl
                        # self.Zipcodes = self.Load_Zipcodes()
                        # for zipcode in self.Zipcodes:
                        #     tmp =[]
                        #     tmp = list(r)
                        #     myzipcode = zipcode[0].replace("\n" , '')
                        #     tmp[2] = myzipcode #for zipcode
                        #     tmp[11] = "1" # for hash
                        #     self.crawl_list_jobs.append(tmp)
                        print("let's do big crawl")
                        # self.crawltype = 2
                        break
                    elif r[6] == 0:
                        # hourly crawl
                        self.crawl_list_jobs.append(r)
                        print("let's do customized")
                        self.crawltype = 1
            elif "livecrawl" in arg_repid:
                #live crawl
                tmp = []
                tmp = list(r)
                tmp[6] = 3
                self.crawl_list_jobs.append(tmp)
                print("let's do live crawl")
                self.crawltype = 3
            elif arg_repid == "test":
                self.crawl_list_jobs.append(r)
                print("let's do some test")
                self.crawltype = 0

        self.IPs = self.Load_IPs()
        print("number of available IPs",len(self.IPs))
    def start_requests(self):
        data = self.crawl_list_jobs
        for cust in self.crawl_list_jobs:
            mydict = {}
            c_t = cust[6]
            mydict["up"] = str(cust[4])
            mydict["zipcode"] = str(cust[2])
            mydict["amount"] = str(cust[3])
            mydict["hash"] = str(cust[11])
            mydict["proxy"] = 'http://' + random.choice(self.IPs)
            mydict["crawl_type"] = self.crawltype
            if len(str(mydict["zipcode"])) > 0:
                url = self.Check_Generated_Url(mydict["zipcode"],mydict["up"],mydict["amount"])
                print("the url is::",url)
                yield scrapy.Request(url=url, callback=self.parse,errback = self.errback_httpbin, meta = mydict,dont_filter = True)

    def errback_httpbin(self, failure):
        now = datetime.datetime.now()
        s = self.myspider_start_at
        diff = now - s
        if failure.request.meta["crawl_type"] == 1 and  diff.seconds > 2400:#customized
            print("<problem> Error : consume more than ONE hour")
            raise CloseSpider("Error ::Too much time for crawling")
        if failure.request.meta["crawl_type"] == 2 and  diff.seconds > 7200:#bigcrawl
            print("<problem> Error : consume more than TWO hour for big crawl")
            raise CloseSpider("code Error ::Too much time for crawling")

        mymeta = failure.request.meta
        mymeta["proxy"] = 'http://' + random.choice(self.IPs)
        return scrapy.Request(url=failure.request.url, callback=self.parse, errback=self.errback_httpbin, meta=mymeta,dont_filter = True)



    def clean_rate(self,rate):
        if rate:
            cleanrate = re.findall(r'[0-9],[0-9]', rate)
            tmp1 = cleanrate[0].replace(',' , '.')
            return float(tmp1)
        else:
            return 0
    def clean_date(self,ddate):
        cleandate = re.findall(r'\d+',ddate)
        return cleandate[0]
    def clear_provide(self,provider):
        print("provider is;;{}".format(provider))
        clear_provide = re.sub('\n', '',provider)
        clear_provide = re.sub('\t' , '' , clear_provide)
        return clear_provide


    def parse(self , response):
        if response.status == 200:
            # f = open("/home/ubuntu/H_H/Crawl_Pars/fastenergy/fastenergy/spiders/" + response.meta["zipcode"],'w')
            # f.write(response.text)
            # f.close()
            print("download latency for fastenergy parse is ::",response.request.meta["download_latency"])
            now = datetime.datetime.now()
            s = self.myspider_start_at
            diff = now - s
            if response.meta["crawl_type"] == 1 and  diff.seconds > 2400:#customized
                print("<problem> Error : consume more than ONE hour")
                raise CloseSpider("code Error ::Too much time for crawling")
            if response.meta["crawl_type"] == 2 and  diff.seconds > 7200:#big crawl
                print("<problem> Error : consume more than TWO hour for big crawl")
                raise CloseSpider("code Error ::Too much time for crawling")

            self.zipcode_done.append(response.meta["zipcode"])

            rate = self.clean_rate(response.xpath("//div[@class='trust-ekomi']/div[2]/strong/text()").extract_first())
            provider_name = self.clear_provide(response.xpath("//div[@class='col-6 col-md-3 col-lg-4 pl-3']/div[1]/text()").extract_first())
            num_of_days = self.clean_date(response.xpath("//td[@class='w-50 pl-0 pl-md-2 feh_order_button_extern']/text()").extract_first())
            xpathquery = ""
            if response.xpath("//div[@id ='feh_produktauswahl2-3']/div"):
                xpathquery = "//div[@id ='feh_produktauswahl2-3']/div"
            elif response.xpath("//div[@id ='feh_produktauswahl2-2']/div"):
                xpathquery = "//div[@id ='feh_produktauswahl2-2']/div"
            for prod_ctr,product in enumerate(response.xpath(xpathquery)):
                item = FastenergyItem()
                if len(response.meta["hash"]) > 1:
                    item['cra_hash'] = response.meta["hash"]
                elif len(response.meta["hash"]) == 1:
                    md5str = str(response.meta["zipcode"]) + '-' + str(response.meta["amount"]) + '-' + str(response.meta["up"]) + '-' + str(prod_ctr)
                    item['cra_hash'] = self.MD5_HASH(md5str.encode('utf-8'))
                item['datetime'] = datetime.datetime.now()
                item['site_id'] = 5
                item['fk_ans'] = 1
                item['zipcode'] = response.meta["zipcode"]
                item['cra_amount'] = response.meta["amount"]
                item['up'] = response.meta["up"]
                item['cra_pro'] = prod_ctr + 1
                item['payment_type'] = 0
                item['cra_express'] = 0
                item['cra_hose'] = 0
                item['cra_short_vehicle'] = 0
    # #**************************************************************
                item['providername'] = provider_name
    # #**************************************************************
                item['deliverydate'] = num_of_days
    # #**************************************************************            
                item['cra_position'] = 0
    # #************************************************************** 
                price = product.xpath(".//div[@class='col-5 text-right fs-1-3 pr-2 pr-lg-3']/strong/text()").extract_first()  
                tmpprice = price.split(' ')[0]
                tmpprice2 = tmpprice.replace(',','.')
                item['price'] = float(tmpprice2)
    # #**************************************************************      
                item['Rate'] = rate
    # #**************************************************************            
                item['Reviews'] = 0
    # #**************************************************************            
                item['crawl_type'] = response.meta["crawl_type"]
                item['fk_dealer'] = 1255487
    # #**************************************************************
                yield item


        else:
            print("we have big <problem> in heizoel24:: status is not 200 in parse() function")
    def Update_IPs(self,ip_list):
        try:
            db_curs,db_conn = self.connection()
            for myip in ip_list:
                query = """UPDATE ips SET ip_status=%s,ip_last_updated_on=%s WHERE ip_name=%s"""
                # query = query.replace("(/myip/)",myip)
                # query = query.replace("(/mydate/)",str(datetime.datetime.now()))
                data = ('0',datetime.datetime.now(),myip)
                # print(query)
                db_curs.execute(query,data)
                db_conn.commit()
            db_curs.close()
            db_conn.close()
        except Exception as e:
            print("Have issue in update IP  ",e)
    def spider_closed(self, spider):
        print("########### lets Update IPs table ####################")
        # self.Update_IPs(self.ip_failed)
        print("#############  update done ###########################")
        print(self.ip_failed)
        print("number of zipcodes DONE ::",len(self.zipcode_done))
        print("Number of failed IPs ::",len(self.ip_failed))
        print(self.zipcode_failed)
#        self.zipcode_done = []
        if len(self.zipcode_done) < len(self.crawl_list_jobs):
            import sys
            sys.path.append('../../../')
            from Controller.sendnotification import SendNotification
            sendObj = SendNotification()
            sendObj.missingAllCrawl("fastenergy",self.crawltype,len(self.zipcode_done),len(self.crawl_list_jobs))
        if len(self.zipcode_done) > 0 and self.crawltype == 3:
            print("let's call PHP process in heizoel at time::",datetime.datetime.now())
            import requests
            r = requests.get("https://www.projektgesellschaft.de/jobs/job_12.php")
        else:
            print("No zipcode done therefore cant call PHP process in heizoel at time::",datetime.datetime.now())

        print("Let's end the crawl crawl at ::",datetime.datetime.now())
        dif = datetime.datetime.now() - self.myspider_start_at
        print("total time for this crawl took ::",dif.seconds)