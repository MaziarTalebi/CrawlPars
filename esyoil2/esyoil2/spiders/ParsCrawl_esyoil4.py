
# -*- coding: utf-8 -*-
import scrapy
import random
import os
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import glob
import lxml.html as lh
from lxml import etree
from esyoil2.items import Esyoil2Item
import datetime
import datefinder
import w3lib.encoding
import MySQLdb
import hashlib
from scrapy.exceptions import CloseSpider


# version of scrapy is 1.5.1
class ParsCrawl_esyoil(scrapy.Spider):
    name="esyoil4"
    # handle_httpstatus_list = [301, 302]
    zipcode_failed = []
    zipcode_done = []
    ip_failed = []
    IPs = []
    Zipcodes = []
    chunk_size = 7
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
    def Load_IPs(self):
        db_curs,db_conn = self.connection()
        query = """SELECT ip_name FROM ips where ip_status='1'"""
        db_curs.execute(query)
        IPS_table = db_curs.fetchall()
        newlist=[]
        for ip in IPS_table:
            newlist.append(ip[0])
        db_curs.close()
        db_conn.close()
        return newlist
    def Load_Zipcodes(self):
        db_curs,db_conn = self.connection()
        query = """SELECT zip_zipcode FROM zipcode"""
        db_curs.execute(query)
        Zipcodes_table = db_curs.fetchall()
        db_curs.close()
        db_conn.close()
        return Zipcodes_table
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
        super(ParsCrawl_esyoil, self).__init__(*args, **kwargs)
        self.myspider_start_at = datetime.datetime.now()
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.crawl_list_jobs = []
        self.crawltype = 0
        self.zipCodeAlexaCrawl = ''
        print(arg_repid)
        if arg_repid == "hourly" or arg_repid == "bigcrawl" or arg_repid == "test":
            req = self.AList_Request()
        elif "livecrawl" in arg_repid:
            newarg_repid = arg_repid.replace("livecrawl","")
            req = self.List_Request(newarg_repid)
        elif 'alexacrawl' in arg_repid:
            self.zipCodeAlexaCrawl = arg_repid.replace('alexacrawl','')
            req = self.AList_Request()
        for r in req:
            start_at,end_at = self.table_time_setting(str(r[7]),str(r[8]))
            now = datetime.datetime.now()
            # now = now.replace(hour=7)
            # now = now.replace(day=8,hour=4)
            # start_at = start_at.replace(day=8)
            # end_at = end_at.replace(day=8)
            if arg_repid == "hourly":
            # whether hourly crawl or big crawl
                if now >= start_at and now <= end_at:
                    if r[6] == 1 and now.weekday() == r[9]:
                        # big crawl
                        self.Zipcodes = self.Load_Zipcodes()
                        for zipcode in self.Zipcodes:
                            tmp = []
                            tmp = list(r)
                            myzipcode = zipcode[0].replace("\n" , '')
                            tmp[2] = myzipcode #for zipcode
                            tmp[11] = "1" # for hash
                            self.crawl_list_jobs.append(tmp)
                        print("let's do big crawl")
                        self.crawltype = 2
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
            elif 'alexacrawl' in arg_repid:
                tmp = list(r)
                tmp[2] = self.zipCodeAlexaCrawl
                tmp[11] = '1'
                print("let's do some test")
                self.crawltype = 5
                self.crawl_list_jobs.append(tmp)
                break
                
        self.IPs = self.Load_IPs()
        print("number of available IPs",len(self.IPs))
        print("Let's start new crawl at ::",datetime.datetime.now())

    def start_requests(self):
        data = self.crawl_list_jobs
        # newlist = [data[x:x+self.chunk_size] for x in range(0, len(data), self.chunk_size)]
        for cust in data:
            mydict = {}
            url = 'https://www.esyoil.com/?calc%5Bunloading_points%5D=(/my_up/)&calc%5Bprod%5D=(/myprod/)&calc%5Bpayment_type%5D=1&calc%5Bshort_vehicle%5D=&calc%5Bhose%5D=&calc%5Bapp%5D=1&calc%5Bzipcode%5D=(/myzipcode/)&calc%5Bamount%5D=(/myamount/)&calc%5Bsubmit%5D='
            myprod = cust[5]
            if myprod == 1:
                myprod = 8
            if myprod == 2:
                myprod = 4
            mydict["prod"] = myprod
            mydict["up"] = str(cust[4])
            mydict["zipcode"] = str(cust[2])
            mydict["amount"] = str(cust[3])
            if cust[11] == "1":#Big Crawl
                md5str = mydict["zipcode"] + '-' + mydict["amount"] + '-' + mydict["up"] + '-' + str(mydict["prod"])
                mydict["hash"] = self.MD5_HASH(md5str.encode('utf-8'))
            else:
                mydict["hash"] = cust[11]
            mydict["crawl_type"] = self.crawltype
            mydict["proxy"] = 'http://' + random.choice(self.IPs)
            url = url.replace('(/myzipcode/)' , mydict["zipcode"])
            url = url.replace('(/my_up/)' , mydict["up"])
            url = url.replace('(/myprod/)' , str(mydict["prod"]))
            url = url.replace('(/myamount/)' , mydict["amount"])
            yield scrapy.Request(url=url, callback=self.parse,errback = self.errback_httpbin,meta = mydict, dont_filter = True)


    def errback_httpbin(self, failure):
        now = datetime.datetime.now()
        s = self.myspider_start_at
        diff = now - s
        if failure.request.meta["crawl_type"] == 1 and  diff.seconds > 2400:#  40 min
            print("<problem>Error : consume more than ONE hour")
            raise CloseSpider("<problem>Error ::Too much time for crawling")
        if failure.request.meta["crawl_type"] == 2 and  diff.seconds> 7200:# 70 min
            print("<problem>Error : consume more than TWO hour for big crawl")
            raise CloseSpider("<problem>Error ::Too much time for crawling")
        myzipcode = failure.request.meta["zipcode"]
        mymeta = failure.request.meta
        if myzipcode not in self.zipcode_failed:
            self.zipcode_failed.append(myzipcode)
        myip = failure.request.meta["proxy"]
        myip = myip.replace("http://","")
        self.ip_failed.append(myip)
        print("let's print the meta ::",failure.request.meta, "and number of fields in meta are:",len(failure.request.meta))
        mymeta["proxy"] = 'http://' + random.choice(self.IPs)
        return scrapy.Request(url=failure.request.url, callback=self.parse, errback=self.errback_httpbin, meta=mymeta,dont_filter = True)
    def parse(self , response):
        print("in parse",response.request.headers['User-Agent'])
        print("dataloss is ::",response.request.meta.get("download_fail_on_dataloss"))
        print("download latency  is ::",response.request.meta["download_latency"])
        print("retry enable is ::",response.request.meta.get('dont_retry'))
        print("size of html is::",len(str(response.text)))
        print("length of xpath is::",len(response.xpath("//li[@class = 'pricelist-entry']")))
        print("response status is::",response.status)
        if response.status == 200 and len(response.xpath("//li[@class = 'pricelist-entry']")) > 0:
            now = datetime.datetime.now()
            s = self.myspider_start_at
            diff = now - s
            if response.meta["crawl_type"] == 1 and  diff.seconds> 2400:
                print("<problem>Error : consume more than ONE hour")
                raise CloseSpider("<problem>Error ::Too much time for crawling")
            if response.meta["crawl_type"] == 2 and  diff.seconds> 7200:
                print("<problem>Error : consume more than TWO hour for big crawl")
                raise CloseSpider("<problem>Error ::Too much time for crawling")
            print("let's pars it")
            print("the status in parse is::",response.status)
            print("number of items in meta::",len(response.meta))
            myzipcode = response.meta["zipcode"]
            items = {}
            items["alllist"] = []
            for ctr,li in enumerate(response.xpath("//li[contains(@class , 'pricelist-entry')]")):
                item = Esyoil2Item()
                item['cra_hash'] = response.meta["hash"]
                item['datetime'] = datetime.datetime.now()
                item['esyoild_site_id'] = 3
                item['fk_ans'] = 1
                item['zipcode'] = myzipcode
                item['cra_amount'] = response.meta["amount"]
                item['up'] = response.meta["up"]
                item['cra_pro'] = response.meta["prod"]
                item['payment_type'] = 0
                item['cra_express'] = 0
                item['cra_hose'] = 0
                item['cra_short_vehicle'] = 0
    #**************************************************************
                provider = li.xpath(".//div[@class='provider-name']/a/text()").extract_first()
                if provider:
                    provider = provider.replace("\xa0"," ")
                else:
                    provider = "Sonderangebot"
                item['providername'] = provider
    #**************************************************************
                # ddate = li.xpath(".//div[@class='col col-delivery workdays']/div/text()").extract_first()
                # ddate = ddate.replace('.','-')
                # item['deliverydate'] = list(datefinder.find_dates(ddate))[0]
    #**************************************************************  
                num_of_days = li.xpath(".//data[@class='focus']/text()").extract_first()
                if num_of_days:
                    num_of_days = num_of_days.replace("\xa0"," ")
                    item['deliverydate'] = num_of_days.split(" ")[0]
                else:
                    item['deliverydate'] = '0'
    #**************************************************************            
                item['cra_position'] = ctr + 1
    #**************************************************************  
                pp = li.xpath(".//data[@class='price']/text()").extract_first()
                if pp:
                    myp = float(pp.replace(',','.'))
                    item['price'] = round(myp,2)
                else:
                    item['price'] = '00.00'
    #************************************************************** 
                if li.xpath(".//data[@class='stars-number']/text()").extract_first():
                    myrate = li.xpath(".//data[@class='stars-number']/text()").extract_first()
                    myrate = myrate.replace(',','.') 
                    myr = float(myrate)
                    item['Rate'] = round(myr,2)
                else:
                    item['Rate'] = 0.0
    #**************************************************************            
                if li.xpath(".//div[@class='rating-count']/text()").extract_first():
                    myreview = li.xpath(".//div[@class='rating-count']/text()").extract_first()
                    myreview = myreview.replace("\xa0"," ").replace("\n","").replace("\t","")
                    if myreview.isdigit():
                        item['Reviews'] = myreview.split(" ")[0]
                    else:
                        item['Reviews'] = 0
                else:
                    item['Reviews'] = 0
    #**************************************************************            
                item['crawl_type'] = response.meta["crawl_type"]
                item['fk_dealer'] = 1255487
    #**************************************************************
                if len(item) == 20:
                    pass
                else:
                    print("<problem> number of fields in item is not 20")
                items["alllist"].append(item)
            self.zipcode_done.append(myzipcode)
            yield items
        elif response.status == 301:
            print("Redirect happened,")
            # response.request.headers.setdefault('User-Agent', ua)
        else:
            print("either status is not 200 or length of html is less and status and length are such as::",response.status,len(response.text))
            mymeta = response.meta
            mymeta["proxy"] = 'http://' + random.choice(self.IPs)
            # f = open(str(response.meta["hash"]) + ".txt",'w')
            # f.write(str(response.text))
            # f.close()
            return scrapy.Request(url=response.url, callback=self.parse, errback=self.errback_httpbin, meta=mymeta,dont_filter = True)            
            
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
            print("<esyoilgotproblem>Have issue in update IP  ",e)
    def spider_closed(self, spider):
        print("########### lets Update IPs table ####################")
        self.Update_IPs(self.ip_failed)
        print("#############  update done ###########################")
        print(self.ip_failed)
        print("number of zipcodes DONE ::",len(self.zipcode_done))
        print("Number of failed IPs ::",len(self.ip_failed))
#        self.zipcode_done = []
        if len(self.zipcode_done) < len(self.crawl_list_jobs) :
            import sys
            sys.path.append('../../../')
            from Controller.sendnotification import SendNotification
            sendObj = SendNotification()
            sendObj.missingAllCrawl("esyoil",self.crawltype,len(self.zipcode_done),len(self.crawl_list_jobs))
            
        if len(self.zipcode_done) > 0 and self.crawltype == 3:
            print("let's call PHP process in heizoel at time::",datetime.datetime.now())
            import requests
            r = requests.get("https://www.projektgesellschaft.de/jobs/job_12.php")
        else:
            print("No zipcode done therefore cant call PHP process in heizoel at time::",datetime.datetime.now())
        diff = datetime.datetime.now() - self.myspider_start_at
        print("total time consumed for esyoil::",diff.seconds)
        print(self.zipcode_failed)
        print("Let's end the crawl crawl at ::",datetime.datetime.now())