import sys
import os
from scrapy.cmdline import execute
def call_crawl():
    os.chdir("/home/ubuntu/H_H/Crawl_Pars/esyoil2/esyoil2/spiders")
    execute(['scrapy', 'crawl', 'esyoil4','-a','arg_repid=alexacrawl'+sys.argv[1]])
    # execute(['scrapy', 'crawl', 'esyoil2'])

if __name__ == "__main__":
    call_crawl()
