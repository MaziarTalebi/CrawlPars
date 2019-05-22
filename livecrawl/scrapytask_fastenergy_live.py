import sys
import os
from scrapy.cmdline import execute
def call_crawl():
    os.chdir("/home/ubuntu/H_H/Crawl_Pars/fastenergy/fastenergy/spiders")
    execute(['scrapy', 'crawl', 'fastenergy1','-a','arg_repid=livecrawl'+sys.argv[1]])
if __name__ == "__main__":
    call_crawl()