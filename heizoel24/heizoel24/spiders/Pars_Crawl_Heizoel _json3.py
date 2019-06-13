##############################################################
##############################################################
##############################################################
import heizolcrawlconfig as hcc
import sys
for customPath in hcc.heizolconfig['path']: 
    if not customPath in sys.path:
        sys.path.append(customPath)
##############################################################
##############################################################
##############################################################
import scrapy
import random
import os
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import lxml.html as lh
from lxml import etree
from heizoel24.items import Heizoel24Item
import datetime
import datefinder
import w3lib.encoding
import hashlib
from scrapy.exceptions import CloseSpider
import re
import json
import html
import xml.sax.saxutils as saxutils
from Model.DataBaseConnection import dbProxy
from Module.TaskHandling import generateListOfRequestsForCrawler, taskControl
from Controller.sendnotification import SendNotification


class Pars_Crawl_Heizoel(scrapy.Spider):
    name = "heizoeljson3"

    chunk_size = 7
    
    def MD5_HASH(self,md5str):
        md5obj=hashlib.md5()
        md5obj.update(md5str)
        return md5obj.hexdigest()
    
    def __init__(self,arg_repid=None,*args, **kwargs):
        super(Pars_Crawl_Heizoel, self).__init__(*args, **kwargs)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.myspider_start_at = datetime.datetime.now()
        self.apikey = ""
        oProxy = dbProxy()
        self.IPs = oProxy.Load_IPs()
        self.zipcode_failed = []
        self.zipcode_done = []
        self.ip_failed = []
        oListForCrawl = generateListOfRequestsForCrawler(arg_repid)
        [self.crawl_list_jobs,self.crawltype] = oListForCrawl.getListForCrawl()
        print(f"number of available IPs {len(self.IPs)}")
    def start_requests(self):
        url = "https://www.heizoel24.de/"
        data = self.crawl_list_jobs
        newlist = [data[x:x+self.chunk_size] for x in range(0, len(data), self.chunk_size)]
        for chunk in newlist:
            dict_list = []
            for cust in chunk:
                mydict = {}
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
            #################################
            #################################
            oTaskControl = taskControl()
            if oTaskControl.hasExceedTimeLimit( response.meta["crawl_type"] , self.myspider_start_at):
                raise CloseSpider("Error ::Too much time for crawling")
            #################################
            #################################
            print(f"""download latency for heizoel parseapikey is :: {response.request.meta["download_latency"]}""")
            print("we are in parseapi")


            self.apikey = response.xpath("//input[@id='_apiat']/@value").extract_first()
            # print("apikey is: ",self.apikey)
            for request in response.meta["dict_list"]:
                print(request)
                url = f"""https://www.heizoel24.de/api/CalculatorAPI/GetPrices?zip={request["zipcode"]}&countryId=1&quantity={request["amount"]}&options=10001-{request["up"]}%2C1-5%2C5-24%2C2-6%2C4-11%2C3-9%2C10002-0&apiToken={self.apikey}&listMode=1&sortMode=0"""
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
        oTaskControl = taskControl()
        if oTaskControl.hasExceedTimeLimit( failure.request.meta["crawl_type"] , self.myspider_start_at):
            raise CloseSpider("Error ::Too much time for crawling")

        myip = failure.request.meta["proxy"]
        myip = myip.replace("http://","")
        self.ip_failed.append(myip)
        mymeta = failure.request.meta
        
        if failure.request.url == "https://www.heizoel24.de/":
            print("<problem> and failure in heizoel")
            print(mymeta)
            mymeta["proxy"] = 'http://' + random.choice(self.IPs)
            return scrapy.Request(url=failure.request.url, callback=self.parseapikey, errback=self.errback_httpbin, meta=mymeta,dont_filter = True)
        elif "CalculatorAPI" in failure.request.url:
            print(f"<problem> in API {failure.request.url} , {failure.request.meta}")
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
            print(f"""download latency for heizoel parse is :: {response.request.meta["download_latency"]}""")
            oTaskControl = taskControl()
            if oTaskControl.hasExceedTimeLimit( response.meta["crawl_type"] , self.myspider_start_at):
                raise CloseSpider("Error ::Too much time for crawling")

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
            print(f"the process done for zipcode:: {myzipcode}")
            if myzipcode in self.zipcode_failed:
                self.zipcode_failed.remove(myzipcode)
            self.zipcode_done.append(myzipcode)
        else:
            print("we have big <problem> in heizoel24:: status is not 200 in parse() function")

    def spider_closed(self, spider):
        oTaskControl = taskControl()
        oTaskControl.spiderClosing(self.ip_failed , self.zipcode_done , self.zipcode_failed , self.crawl_list_jobs , self.crawltype , self.myspider_start_at)
