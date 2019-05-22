import sys
import os
from scrapy.cmdline import execute
from subprocess import Popen
# def run_portals(argstr):
#     os.chdir("/home/ubuntu/H_H/Crawl_Pars/esyoil2/esyoil2/spiders")
#     execute(['scrapy', 'crawl', 'esyoil4','-a','arg_repid=' + argstr])

#     os.chdir("/home/ubuntu/H_H/Crawl_Pars/heizoel24/heizoel24/spiders")
#     execute(['scrapy', 'crawl', 'heizoeljson3','-a','arg_repid=' + argstr])

#     # esyoilcmd = ['scrapy', 'crawl', 'esyoil4','-a','arg_repid=' + argstr]
#     # Popen(esyoilcmd,cwd = "/home/ubuntu/H_H/Crawl_Pars/esyoil2/esyoil2/spiders")
#     # heizoelcmd = ['scrapy', 'crawl', 'heizoeljson3','-a','arg_repid=' + argstr]
#     # Popen(heizoelcmd,cwd = "/home/ubuntu/H_H/Crawl_Pars/heizoel24/heizoel24/spiders")
def call_crawl():
    argstr = sys.argv[1]

    os.chdir("/home/ubuntu/H_H/Crawl_Pars/esyoil2/esyoil2/spiders")
    execute(['scrapy', 'crawl', 'esyoil4','-a','arg_repid=' + argstr])
    
    os.chdir("/home/ubuntu/H_H/Crawl_Pars/heizoel24/heizoel24/spiders")
    execute(['scrapy', 'crawl', 'heizoeljson3','-a','arg_repid=' + argstr])



if __name__ == "__main__":
    call_crawl()