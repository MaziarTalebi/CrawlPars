import scrapy
import random
import os
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import glob
import lxml.html as lh
from lxml import etree
from heizoel24.items import Heizoel24Item
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

class Pars_Crawl_Heizoel(scrapy.Spider):
    name = "heizoeljson3"
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
        return newlist
    def Load_Zipcodes(self):
        db_curs,db_conn = self.connection()
        query = """SELECT zip_zipcode FROM zipcode"""
        db_curs.execute(query)
        Zipcodes_table = db_curs.fetchall()
        return Zipcodes_table
    def AList_Request(self):
        db_curs,db_conn = self.connection()
        query = """SELECT * FROM requests_python"""
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
        super(Pars_Crawl_Heizoel, self).__init__(*args, **kwargs)
        self.apikey = ""
        self.myspider_start_at = datetime.datetime.now()
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.crawl_list_jobs = []
        self.crawltype = 0
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
            # now = now.replace(day=4,hour=4)
            # start_at = start_at.replace(day=4)
            # end_at = end_at.replace(day=4)
            if arg_repid == "hourly":
                # whether hourly crawl or big crawl
                if now >= start_at and now <= end_at:
                    if r[6] == 1 and now.weekday() == r[9]:
                        # big crawl
                        self.Zipcodes = self.Load_Zipcodes()
                        for zipcode in self.Zipcodes:
                            tmp =[]
                            tmp = list(r)
                            myzipcode = zipcode[0].replace("\n" , '')
                            tmp[2] = myzipcode #for zipcode
                            tmp[11] = "1" # for hash
                            self.crawl_list_jobs.append(tmp)
                        print("let's do big crawl")
                        self.crawltype = 2
                        break
                    elif r[6] == 0:
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
                print('the tmp is::',tmp)
                self.crawltype = 5
                self.crawl_list_jobs.append(tmp)
                break

        self.IPs = self.Load_IPs()
        print("number of available IPs",len(self.IPs))
    def start_requests(self):
        url = "https://www.heizoel24.de/"
        data = self.crawl_list_jobs
        newlist = [data[x:x+self.chunk_size] for x in range(0, len(data), self.chunk_size)]
        for chunk in newlist:
            dict_list = []
            for cust in chunk:
                mydict = {}
                c_t = cust[6]
                mydict["up"] = str(cust[4])
                mydict["zipcode"] = str(cust[2])
                mydict["amount"] = str(cust[3])
                mydict["hash"] = str(cust[11])
                dict_list.append(mydict)
            req = scrapy.Request(url=url, callback=self.parseapikey,errback = self.errback_httpbin, dont_filter = True)
            req.meta["dict_list"] = dict_list
            req.meta["crawl_type"] = self.crawltype
            req.meta["proxy"] = 'http://' + random.choice(self.IPs)
            yield req
    def parseapikey(self , response):
        if response.status == 200:
            print("download latency for heizoel parseapikey is ::",response.request.meta["download_latency"])
            print("we are in parseapi")
            now = datetime.datetime.now()
            s = self.myspider_start_at
            diff = now - s
            if response.meta["crawl_type"] == 1 and  diff.seconds > 2400:#customized
                print("<problem> Error : consume more than ONE hour")
                raise CloseSpider("Error ::Too much time for crawling")
            if response.meta["crawl_type"] == 2 and  diff.seconds > 7200:#big crawl
                print("<problem> Error : consume more than TWO hour for big crawl")
                raise CloseSpider("code Error ::Too much time for crawling")
            self.apikey = response.xpath("//input[@id='_apiat']/@value").extract_first()
            # print("apikey is: ",self.apikey)
            for request in response.meta["dict_list"]:
                print(request)
                url = "https://www.heizoel24.de/api/CalculatorAPI/GetPrices?zip=(/zip/)&countryId=1&quantity=(/quantity/)&options=10001-(/up/)%2C1-5%2C5-24%2C2-6%2C4-11%2C3-9%2C10002-0&apiToken=(/apikey/)&listMode=1&sortMode=0"
                url = url.replace("(/apikey/)",self.apikey)
                url = url.replace('(/zip/)',request["zipcode"])
                url = url.replace("(/quantity/)",request["amount"])
                url = url.replace("(/up/)",request["up"])
                # mymeta = response.meta
                newmeta = {}
                newmeta["zipcode"] = request["zipcode"]
                newmeta["amount"] = request["amount"]
                newmeta["up"] = request["up"]
                newmeta["hash"] = request["hash"]
                newmeta["proxy"] = response.meta["proxy"]
                newmeta["crawl_type"] = response.meta["crawl_type"]
                yield scrapy.Request(url=url, callback=self.parse,errback = self.errback_httpbin, meta=newmeta,dont_filter = True)
        else:
            print("we have big <problem> in heizoel24:: status is not 200 in parseapikey() function")
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
        
        if failure.request.url == "https://www.heizoel24.de/":
            print("<problem> and failure in heizoel")
            print(mymeta)
            mymeta["proxy"] = 'http://' + random.choice(self.IPs)
            return scrapy.Request(url=failure.request.url, callback=self.parseapikey, errback=self.errback_httpbin, meta=mymeta,dont_filter = True)
        elif "CalculatorAPI" in failure.request.url:
            print("<problem> in API",failure.request.url,failure.request.meta)
            print(mymeta)
            return scrapy.Request(url=failure.request.url, callback=self.parse, errback=self.errback_httpbin, meta=mymeta,dont_filter = True)
        else:
            print("<big problem>")
    def clean_rate(self,rate):
        cleanrate = re.findall(r'\d+', rate)
        return cleanrate[0]
    def clean_date(self,ddate):
        cleandate = ddate.split('Werktage')[0]
        cleandate = self.clean_rate(cleandate)
        return cleandate
    def clear_provide(self,provider):
        return re.sub(' +', ' ',provider)


    def parse(self , response):
        if response.status == 200:
            print("we are in parsjson")
            print(response.meta)
            print("download latency for heizoel parse is ::",response.request.meta["download_latency"])
            now = datetime.datetime.now()
            s = self.myspider_start_at
            diff = now - s
            if response.meta["crawl_type"] == 1 and  diff.seconds > 2400:#customized
                print("<problem> Error : consume more than ONE hour")
                raise CloseSpider("code Error ::Too much time for crawling")
            if response.meta["crawl_type"] == 2 and  diff.seconds > 7200:#big crawl
                print("<problem> Error : consume more than TWO hour for big crawl")
                raise CloseSpider("code Error ::Too much time for crawling")
            html_text = response.body.decode("utf-8")
            html_text = html_text.replace("""<string xmlns="http://schemas.microsoft.com/2003/10/Serialization/">""","")
            html_text = html_text.replace("""</string>""","")
            html_text = html.unescape(html_text)
            html_text = html_text.replace("\\r","")
            html_text = html_text.replace("\\n","")
            html_text = html_text.replace("\\r\\n","")
            html_text = html_text.rstrip()
            data = json.loads(html_text)
            self.zipcode_done.append(response.meta["zipcode"])
            for prod_ctr,product in enumerate(data):
                for dealer_ctr,dealer in enumerate(product["List"]):
                    item = Heizoel24Item()
                    if len(response.meta["hash"]) > 1:
                        item['cra_hash'] = response.meta["hash"]
                    elif len(response.meta["hash"]) == 1:
                        md5str = str(response.meta["zipcode"]) + '-' + str(response.meta["amount"]) + '-' + str(response.meta["up"]) + '-' + str(prod_ctr)
                        item['cra_hash'] = self.MD5_HASH(md5str.encode('utf-8'))
                    item['datetime'] = datetime.datetime.now()
                    item['site_id'] = 4
                    item['fk_ans'] = 1
                    item['zipcode'] = response.meta["zipcode"]
                    item['cra_amount'] = response.meta["amount"]
                    item['up'] = response.meta["up"]
                    item['cra_pro'] = prod_ctr + 1
                    item['payment_type'] = 0
                    item['cra_express'] = 0
                    item['cra_hose'] = 0
                    item['cra_short_vehicle'] = 0
        #**************************************************************
                    item['providername'] = dealer["DealerName"].replace("&amp;","&")
        #**************************************************************
                    item['deliverydate'] = dealer["DeliveryPeriodDays"]#number of days
        #**************************************************************            
                    item['cra_position'] = dealer_ctr + 1
        #**************************************************************   
                    myp = float(dealer["ProductUnitPrice"])         
                    item['price'] = round(myp,2)
        #**************************************************************      
                    myr = dealer["DealerRating"]      
                    item['Rate'] = round(myr,2)
        #**************************************************************            
                    item['Reviews'] = dealer["DealerRatingCount"]
        #**************************************************************            
                    item['crawl_type'] = response.meta["crawl_type"]
                    item['fk_dealer'] = 1255487
        #**************************************************************
                    yield item
            myzipcode = response.meta["zipcode"]
            print("the process done for zipcode::",myzipcode)
            if myzipcode in self.zipcode_failed:
                self.zipcode_failed.remove(myzipcode)
            self.zipcode_done.append(myzipcode)
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
            sendObj.missingAllCrawl("heizoel",self.crawltype,len(self.zipcode_done),len(self.crawl_list_jobs))
        if len(self.zipcode_done) > 0 and self.crawltype == 3:
            print("let's call PHP process in heizoel at time::",datetime.datetime.now())
            import requests
            r = requests.get("https://www.projektgesellschaft.de/jobs/job_12.php")
        else:
            print("No zipcode done therefore cant call PHP process in heizoel at time::",datetime.datetime.now())

        print("Let's end the crawl crawl at ::",datetime.datetime.now())
        dif = datetime.datetime.now() - self.myspider_start_at
        print("total time for this crawl took ::",dif.seconds)