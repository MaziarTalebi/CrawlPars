# -*- coding: utf-8 -*-
import sys
import os
from scrapy.cmdline import execute

os.chdir("/home/ubuntu/H_H/Crawl_Pars/esyoil2/esyoil2/spiders")
execute(['scrapy', 'crawl', 'esyoil4','-a','arg_repid=hourly'])

