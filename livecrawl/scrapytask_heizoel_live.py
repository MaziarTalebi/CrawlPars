import sys
import os
from scrapy.cmdline import execute
def call_crawl():
    print("the arguments are: ",sys.argv)
    os.chdir("/home/ubuntu/H_H/Crawl_Pars/heizoel24/heizoel24/spiders")
    execute(['scrapy', 'crawl', 'heizoeljson3','-a','arg_repid=livecrawl'+sys.argv[1]])
if __name__ == "__main__":
    call_crawl()