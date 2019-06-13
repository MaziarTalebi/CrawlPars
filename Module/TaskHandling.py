##############################################################
##############################################################
##############################################################
import heizolcrawlconfig as hcc
import sys
if not hcc.heizolconfig['path'] in sys.path:
	sys.path.append(hcc.heizolconfig['path'])
##############################################################
##############################################################
##############################################################
import datetime
from Model.DataBaseConnection import dbRequestsPython, dbZipcodes
from Controller.sendnotification import SendNotification


class generateListOfRequestsForCrawler:


    def __init__(self,arg_repid):
        self.arg_repid = arg_repid
        self.crawl_list_jobs = []
        self.Zipcodes = []
        self.crawltype = 9
        self.zipCodeAlexaCrawl = ''
        self.apikey = ""
    def getListForCrawl(self):
        self.prepareListForCrawl()
        return [self.crawl_list_jobs, self.crawltype]
    def prepareListForCrawl(self):
        now = datetime.datetime.now()
        self.myspider_start_at = datetime.datetime.now()
        if self.arg_repid == 'hourly':
            oRequest = dbRequestsPython()
            for r in oRequest.List_Request():
                start_at,end_at = self.table_time_setting(str(r[7]),str(r[8]))
                if now >= start_at and now <= end_at:
                    if r[6] == 1 and now.weekday() == r[9]:
                        # big crawl
                        oZipcode = dbZipcodes()
                        for zipcode in oZipcode.Load_Zipcodes():
                            tmp =[]
                            tmp = list(r)
                            myzipcode = zipcode[0].replace("\n" , '')
                            tmp[2] = myzipcode       #for zipcode
                            tmp[11] = "1"            #for hash
                            tmp[6] = 2               #set crawltype = 2
                            self.crawl_list_jobs.append(tmp)
                        print("let's do bigcrawl")
                        self.crawltype = 2
                        break
                    elif r[6] == 0:
                        tmp = []
                        tmp = list(r)
                        tmp[6] = 1               #set crawltype = 1
                        self.crawl_list_jobs.append(r)
                        self.crawltype = 1
                        print("let's do hourly")

        elif "livecrawl" in self.arg_repid:
            newarg_repid = self.arg_repid.replace("livecrawl","")
            oRequest = dbRequestsPython()
            for r in oRequest.List_Request(newarg_repid):
                tmp = []
                tmp = list(r)
                tmp[6] = 3
                self.crawl_list_jobs.append(tmp)
                self.crawltype = 3
            print("let's do live crawl")
        elif self.arg_repid == "test":
            oRequest = dbRequestsPython()
            for r in oRequest.List_Request():
                start_at,end_at = self.table_time_setting(str(r[7]),str(r[8]))
                if now >= start_at and now <= end_at:
                    if r[6] == 1 and now.weekday() == r[9]:
                        # big crawl
                        oZipcode = dbZipcodes()
                        for zipcode in oZipcode.Load_Zipcodes():
                            tmp =[]
                            tmp = list(r)
                            myzipcode = zipcode[0].replace("\n" , '')
                            tmp[2] = myzipcode       #for zipcode
                            tmp[11] = "1"            #for hash
                            tmp[6] = 0               #set crawltype = 0
                            self.crawl_list_jobs.append(tmp)
                            self.crawltype = 0
                        print("let's do bigcrawl")
                        # self.crawltype = 2
                        break
                    elif r[6] == 0:
                        tmp = []
                        tmp = list(r)
                        tmp[6] = 0               #set crawltype = 0
                        self.crawl_list_jobs.append(r)
                        self.crawltype = 0
                        print("let's do test")
        
        elif 'alexacrawl' in self.arg_repid:
            self.zipCodeAlexaCrawl = self.arg_repid.replace('alexacrawl','')
            oRequest = dbRequestsPython()
            for r in oRequest.List_Request():
                tmp = list(r)
                tmp[2] = self.zipCodeAlexaCrawl
                tmp[11] = '1'
                tmp[6] = 5
                self.crawl_list_jobs.append(tmp)
                self.crawltype = 5
                break

    def table_time_setting(self,start_str,end_str):
        start_list = start_str.split(':')
        end_list = end_str.split(':')
        tmp = datetime.datetime.now()
        start_at_time = tmp.replace(hour=int(start_list[0]),minute=int(start_list[1]),second=int(start_list[2]),microsecond=0)
        end_at_time = tmp.replace(hour=int(end_list[0]),minute=int(end_list[1]),second=int(end_list[2]),microsecond=0)
        return start_at_time,end_at_time

class taskControl:
    def __init__(self):
        pass
    def hasExceedTimeLimit(self,crawltype,myspider_start_at):
        diff = datetime.datetime.now() - myspider_start_at
        if crawltype == 1 and  diff.seconds > 2400:#customized
            print("<problem> Error : consume more than 2400seconds")
            return True
        if crawltype == 2 and  diff.seconds > 7200:#bigcrawl
            print("<problem> Error : consume more than 7200seconds for big crawl")
            return True
        return False

    def spiderClosing(self,ip_failed , zipcode_done , zipcode_failed , crawl_list_jobs , crawltype , myspider_start_at):
        print("########### lets Update IPs table ####################")
        # self.Update_IPs(self.ip_failed)
        print("#############  update done ###########################")
        print(ip_failed)
        print("number of zipcodes DONE ::",len(zipcode_done))
        print("Number of failed IPs ::",len(ip_failed))
        print(zipcode_failed)
#        self.zipcode_done = []
        if len(zipcode_done) < len(crawl_list_jobs):
            sendObj = SendNotification()
            sendObj.missingAllCrawl("heizoel",crawltype,len(zipcode_done),len(crawl_list_jobs))
        if len(zipcode_done) > 0 and crawltype == 3:# livecrawl
            print("let's call PHP process in heizoel at time::",datetime.datetime.now())
            import requests
            requests.get("https://www.projektgesellschaft.de/jobs/job_12.php")
        else:
            print("No zipcode done therefore cant call PHP process in heizoel at time::",datetime.datetime.now())

        print("Let's end the crawl crawl at ::",datetime.datetime.now())
        dif = datetime.datetime.now() - myspider_start_at
        print("total time for this crawl took ::",dif.seconds)