# -*- coding: utf-8 -*-
import sys
import os
from scrapy.cmdline import execute

os.chdir("/home/ubuntu/H_H/Crawl_Pars/fastenergy/fastenergy/spiders")
execute(['scrapy', 'crawl', 'fastenergy1','-a','arg_repid=hourly'])
