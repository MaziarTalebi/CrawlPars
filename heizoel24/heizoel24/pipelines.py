# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html



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

from Model.DataBaseConnection import dbCrawl


import MySQLdb
import MySQLdb.cursors
import hashlib
import sys
import datetime

class Heizoel24Pipeline(object):

    def process_item(self, item, spider):
        print("Let's process this item")
        oDbCrawl = dbCrawl()
        oDbCrawl.Insert_heizol(item)
        return item
