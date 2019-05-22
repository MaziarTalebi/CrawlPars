# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Esyoil2Item(scrapy.Item):
    # define the fields for your item here like:
    cra_hash = scrapy.Field()
    datetime = scrapy.Field()
    esyoild_site_id = scrapy.Field()
    fk_ans = scrapy.Field()
    zipcode = scrapy.Field()
    cra_amount = scrapy.Field()
    up = scrapy.Field()
    cra_pro = scrapy.Field()
    payment_type = scrapy.Field()
    cra_express = scrapy.Field()
    cra_hose = scrapy.Field()
    cra_short_vehicle = scrapy.Field()
    providername = scrapy.Field()
    deliverydate = scrapy.Field()
    cra_position = scrapy.Field()
    price = scrapy.Field()
    Rate = scrapy.Field()
    Reviews = scrapy.Field()
    fk_dealer = scrapy.Field()  
    crawl_type = scrapy.Field()
    others = scrapy.Field()