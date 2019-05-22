from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.proxy import Proxy, ProxyType
import sys
import time
import random
import datetime
import MySQLdb
import hashlib
import os
# def setdefault(ip_proxy):
#     display = Display(visible=0, size=(1366, 768))
#     display.start()
#     options = Options()
#     options.add_argument('-headless')
#     desired_capability = webdriver.DesiredCapabilities.FIREFOX
#     desired_capability['proxy'] = {
#             'proxyType': "manual",
#             'httpProxy': ip_proxy,
#             'ftpProxy': ip_proxy,
#             'sslProxy': ip_proxy,
#         }
#     browser = webdriver.Firefox(firefox_options=options,capabilities=desired_capability)
#     browser.implicitly_wait(7)
#     browser.set_window_size(1366, 2000)
#     browser.get("https://www.fastenergy.de/")
#     time.sleep(1)
#     return browser,display
def setdefault(ip_proxy):
    s0 = time.time()
    proxy = ip_proxy.split(":")[0]
    port = ip_proxy.split(":")[1]
    profile = webdriver.FirefoxProfile()
    s1 = time.time()
    profile.set_preference("network.proxy.type", 1)
    profile.set_preference("network.proxy.http", proxy)
    profile.set_preference("network.proxy.http_port", port)
    profile.set_preference("network.proxy.ssl", proxy)
    profile.set_preference("network.proxy.ssl_port", port)
    profile.set_preference('permissions.default.image', 2)
    # profile.update_preferences()
    s3 = time.time()
    browser = webdriver.Firefox(firefox_profile=profile)
    s4 = time.time()
    browser.implicitly_wait(7)
    browser.set_window_size(1366, 2000)
    s5 = time.time()
    print("seting done")
    browser.get("https://www.fastenergy.de/")
    time.sleep(0.8)
    s6 = time.time()
    print("time taken in setdefault::" , s6 - s5, s5 - s4, s4 - s3, s3 - s1,s1 - s0  ) 

    return browser
def finish(browser):
    browser.close()
    browser.quit()

def connection():
    db_conn=MySQLdb.connect(host="35.158.85.11",
                         port=3306,
                         user="mazi",
                         passwd="mazi2crawler",
                         db="crawler",
                         use_unicode=True,
                         charset='utf8')
    db_curs=db_conn.cursor()
    return db_curs,db_conn
def Insert_Generated_Url(zipcode,up,amount,url,site_id,proxy):
    # print("let's insert these data:::",zipcode,up,amount,url,site_id)
    db_curs,db_conn = connection()
    db_curs.execute("""INSERT INTO Generated_URL VALUES(%s,%s,%s,%s,%s,%s)""",(zipcode,up,amount,url,site_id,proxy))
    db_conn.commit()
    db_curs.close()
    db_conn.close()
def Load_All_Generated():
    db_curs,db_conn = connection()
    query = "SELECT * FROM Generated_URL"
    db_curs.execute(query)
    generated_list = db_curs.fetchall()
    newlist = []
    for gl in generated_list:
        newlist.append(list(gl))
    db_curs.close()
    db_conn.close()
    return newlist
def Has_Been_Generated(zipcode,up,amount):
    gen_list = Load_All_Generated()
    url = ""
    for gl in gen_list:
        if str(gl[0]) ==  str(zipcode) and str(gl[1]) ==  str(up) and str(gl[2]) == str(amount):
            url = gl[3]
            break
    return url

def Load_IPs():
    db_curs,db_conn = connection()
    query = """SELECT ip_name FROM ips where ip_status='1'"""
    db_curs.execute(query)
    IPS_table = db_curs.fetchall()
    newlist = []
    db_curs.close()
    db_conn.close()
    for ip in IPS_table:
        newlist.append(ip[0])
    return newlist
def Load_Zipcodes():
    db_curs,db_conn = connection()
    query = """SELECT zip_zipcode FROM zipcode"""
    db_curs.execute(query)
    Zipcodes_table = db_curs.fetchall()
    db_curs.close()
    db_conn.close()
    return Zipcodes_table
def AList_Request():
    db_curs,db_conn = connection()
    query = """SELECT * FROM requests_python"""
    db_curs.execute(query)
    req = db_curs.fetchall()
    db_curs.close()
    db_conn.close()
    return req
def List_Request(my_rep_id):
    my_rep_id = my_rep_id.replace(" ","")
    db_curs,db_conn = connection()
    query = """SELECT * FROM requests_python where rep_id in %s"""
    arg = []
    for a in my_rep_id.split(","):
        arg.append(a)
    totalarg = [arg]
    db_curs.execute(query,totalarg)
    req = db_curs.fetchall()
    db_curs.close()
    db_conn.close()
    return req
def MD5_HASH(md5str):
    md5obj=hashlib.md5()
    md5obj.update(md5str)
    return md5obj.hexdigest()
def table_time_setting(start_str,end_str):
    start_list = start_str.split(':')
    end_list = end_str.split(':')
    tmp = datetime.datetime.now()
    start_at_time = tmp.replace(hour=int(start_list[0]),minute=int(start_list[1]),second=int(start_list[2]),microsecond=0)
    end_at_time = tmp.replace(hour=int(end_list[0]),minute=int(end_list[1]),second=int(end_list[2]),microsecond=0)
    return start_at_time,end_at_time
def crawlfastenergy(browser,zipcode,amount,up):
    try:
        print(zipcode,amount,up)
        s = time.time()
        plz = browser.find_element_by_id("plz")
        plz.send_keys(Keys.CONTROL, "a")
        plz.send_keys(Keys.DELETE)
        plz.send_keys(zipcode)
        menge = browser.find_element_by_id("menge")
        menge.send_keys(Keys.CONTROL, "a")
        menge.send_keys(Keys.DELETE)
        menge.send_keys(amount)
        # abladestellen = browser.find_element_by_id("abladestellen")
        # abladestellen.send_keys(up)
        plz.send_keys(Keys.RETURN)
        time.sleep(0.6)
        # browser.save_screenshot(zipcode + "_1" + '.png')
        print("done for ",zipcode)
        current_url = browser.current_url
        # browser.back()
        # time.sleep(0.4)
        print("time in crawlfastenergy function::",time.time()- s)
        
    except NoSuchElementException:
        print("missed this crawl")
        current_url = "????"
    finally:
        return current_url
filePath = '/home/ubuntu/H_H/Crawl_Pars/fastenergy/fastenergy2' + '/' + str(datetime.datetime.now()) + '.txt'
print(filePath)
f = open(filePath,'w')
f.write('start fast energy')
f.close()
starttime = time.time()
req = AList_Request()
arg_repid = "test"
crawl_list_jobs = []
for r in req:
    start_at,end_at = table_time_setting(str(r[7]),str(r[8]))
    now = datetime.datetime.now()
    # now = now.replace(hour=7)
    # now = now.replace(day=8,hour=4)
    # start_at = start_at.replace(day=8)
    # end_at = end_at.replace(day=8)

    if arg_repid == "hourly":
    # whether hourly crawl or big crawl
        if now >= start_at and now <= end_at:
            if r[6] == 1 and now.weekday() == r[9]:
                # big crawl
                Zipcodes = Load_Zipcodes()
                for zipcode in Zipcodes:
                    tmp = []
                    tmp = list(r)
                    myzipcode = zipcode[0].replace("\n" , '')
                    tmp[2] = myzipcode #for zipcode
                    tmp[11] = "1" # for hash
                    crawl_list_jobs.append(tmp)
                print("let's do big crawl")
                crawltype = 2
                break
            elif r[6] == 0:
                crawl_list_jobs.append(r)
                print("let's do customized")
                crawltype = 1
    elif "livecrawl" in arg_repid:
        #live crawl
        tmp = []
        tmp = list(r)
        tmp[6] = 3
        crawl_list_jobs.append(tmp)
        print("let's do live crawl")
        crawltype = 3
    elif arg_repid == "test":
        crawl_list_jobs.append(r)
#        print("let's do some test")
        crawltype = 0

# IPs = Load_IPs()
# print("number of available IPs",len(IPs))
print("Let's start new crawl at ::",datetime.datetime.now())

data = crawl_list_jobs
print("len of datalist is ::",len(data))
# newlist = [data[x:x+self.chunk_size] for x in range(0, len(data), self.chunk_size)]
ipslist = Load_IPs()

list_url = []
ctr = 0
print("let's start crawl for each request")
display = Display(visible=0, size=(1366, 768))
display.start()
for cust in data:

    ss = time.time()
    ctr = ctr + 1
    print(ctr)
    mydict = {}
    myprod = cust[5]
    mydict["prod"] = myprod
    mydict["up"] = str(cust[4])
    mydict["zipcode"] = str(cust[2])
    mydict["amount"] = str(cust[3])
    genurl = Has_Been_Generated(mydict["zipcode"],mydict["up"],mydict["amount"])
    if len(genurl) > 0:
        print("we have already generated url before")
        continue
    if cust[11] == "1":#Big Crawl
        md5str = mydict["zipcode"] + '-' + mydict["amount"] + '-' + mydict["up"] + '-' + str(mydict["prod"])
        mydict["hash"] = MD5_HASH(md5str.encode('utf-8'))
    else:
        mydict["hash"] = cust[11]
    mydict["crawl_type"] = crawltype
    # Popen(['/usr/bin/python3', '/home/ubuntu/H_H/Crawl_Pars/fastenergy/fastenergy2/newsel.py',mydict["zipcode"],mydict["amount"],mydict["up"]])
    if len(mydict["zipcode"]) >= 4:
        # browser.save_screenshot(mydict["zipcode"] + "_0" + '.png')
        selectedip = random.choice(ipslist)
        print(selectedip)
        browser = setdefault(selectedip)
        current_url = crawlfastenergy(browser,mydict["zipcode"],mydict["amount"],mydict["up"])
        # list_url.append([mydict["zipcode"],mydict["amount"],mydict["up"],current_url])
        Insert_Generated_Url(mydict["zipcode"],mydict["up"],mydict["amount"],current_url,5,selectedip)
        # browser.save_screenshot(mydict["zipcode"] + "_2" + '.png')
        finish(browser)
    print("time taken for this request is::",time.time() - ss)
display.stop()
print(list_url)
# print("Done all the open process in ::",time.time() - starttime)
# f = open("H_H/Crawl_Pars/fastenergy/fastenergy2/listofurl.txt","w")
# for li in list_url:
#     f.write(li.strip())
#     f.write("\n")
# f.close()