import sys
import os
from scrapy.cmdline import execute

os.chdir("/home/ubuntu/H_H/Crawl_Pars/heizoel24/heizoel24/spiders")
execute(['scrapy', 'crawl', 'heizoeljson3','-a','arg_repid=hourly'])
