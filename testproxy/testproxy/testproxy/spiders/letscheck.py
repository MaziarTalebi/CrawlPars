# -*- coding: utf-8 -*-
import scrapy
import MySQLdb
import random

class LetscheckSpider(scrapy.Spider):
    name = 'letscheck'
    start_urls = ['https://whatismyipaddress.com//']
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
    def start_requests(self):
        IPs = self.Load_IPs()
        
        mydict = {}
        mydict["proxy"] = 'http://' + random.choice(IPs)
        print(mydict["proxy"])
        yield scrapy.Request(url='https://whatismyipaddress.com//', callback=self.parse,meta = mydict, dont_filter = True)
    def parse(self, response):
        print("url is::",response.url)
        elm = response.xpath(".//div[@id='ipv4']/a/text()").extract_first()
        print(elm)
        
