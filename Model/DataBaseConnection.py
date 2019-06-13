
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



from conf import config
import MySQLdb
import datetime
import hashlib



class dbConnection:

    def __init__(self):
        self.host = config.DATABASE_CONFIG['host']
        self.port = config.DATABASE_CONFIG['port']
        self.user = config.DATABASE_CONFIG['user']
        self.DB = config.DATABASE_CONFIG['dbname']
        self.password = config.DATABASE_CONFIG['password']
        self.db_conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=self.DB, use_unicode=True, charset='utf8')
        self.db_curs = self.db_conn.cursor()

    def __del__(self):
        self.db_curs.close()
        self.db_conn.close()
        


class dbCrawl(dbConnection):
    def MD5_HASH(self,md5str):
        md5obj=hashlib.md5()
        md5obj.update(md5str)
        return md5obj.hexdigest()
    def Insert_heizol(self,item):
        oDealor = dbDealer()
        try:
            fk_dealer = oDealor.Load_Dealer(item['providername'] , item['site_id'])
            print("Dealer:: ",fk_dealer,"providername::",item['providername'])
            if fk_dealer == -1 :
                print("couldnt find ::",item['providername'])
                oDealor.Update_Dealer(item['providername'], item['site_id'])
                fk_dealer = oDealor.Load_Dealer(item['providername'] , item['site_id'])
                print("we did update and get the id ::",fk_dealer)
                item['fk_dealer'] = fk_dealer
            elif fk_dealer >= 0:
                print("only one found for ::",item['providername'])
                print(fk_dealer)
                item['fk_dealer'] = fk_dealer
            elif fk_dealer < -1:
                print("<problem>sth wrong with dealer")
            #md5_str = str(item['zipcode'])+'-'+str(item['cra_amount'])+'-'+str(item['up'])+'-'+str(item['cra_pro'])
            self.db_curs.execute("""INSERT INTO crawls VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                         ('0',item['cra_hash'],item['datetime'],item['site_id'],item['fk_ans'],item['zipcode'],item['cra_amount'],item['up'],item['cra_pro'],item['payment_type'],item['cra_express'],item['cra_hose'],item['cra_short_vehicle'],item['providername'],item['deliverydate'],item['cra_position'],item['price'],item['Rate'],item['Reviews'],item['fk_dealer'],item['crawl_type']))
            print("Done:::"+str(item['zipcode'])+' '+str(item['cra_amount']))
            self.db_conn.commit()
        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print ("<problem>Time of the exception code ERROR",datetime.datetime.now(),e)
        finally:
            print ("<<<<<<<<<<<<< Here is FINALLY  >>>>>>>>>>")







class dbDealer(dbConnection):
    def Load_Dealer(self,company_name , siteid):
        try:
            result = -1
            query = """SELECT dea_id,dea_name FROM dealer where dea_fk_sit='(/siteid/)'"""
            query = query.replace('(/siteid/)',str(siteid))
            self.db_curs.execute(query)
            result_table = self.db_curs.fetchall()
            for my_id,my_name in result_table:
                if my_name.replace("\xa0"," ") == company_name:
                    result = my_id
                    break
        except Exception as e:
            result = -1000
            print("<problem> error in load dealer with providername::",company_name,e)
        finally:
            pass
        return result

    def Update_Dealer(self,company_name,site_id):
        self.db_curs.execute("""INSERT INTO dealer VALUES(%s,%s,%s,%s)""",('0',company_name,datetime.datetime.now(),str(site_id)))
        self.db_conn.commit()



class dbGeneratedUrl(dbConnection):
    pass



class dbProxy(dbConnection):
    def Load_IPs(self):
        query = """SELECT ip_name FROM ips where ip_status='1'"""
        self.db_curs.execute(query)
        IPS_table = self.db_curs.fetchall()
        newlist=[]
        for ip in IPS_table:
            newlist.append(ip[0])
        return newlist
    def Update_IPs(self,ip_list):
        try:
            for myip in ip_list:
                query = """UPDATE ips SET ip_status=%s,ip_last_updated_on=%s WHERE ip_name=%s"""
                # query = query.replace("(/myip/)",myip)
                # query = query.replace("(/mydate/)",str(datetime.datetime.now()))
                data = ('0',datetime.datetime.now(),myip)
                # print(query)
                self.db_curs.execute(query,data)
                self.db_conn.commit()
        except Exception as e:
            print("Have issue in update IP  ",e)



class dbRequestsPython(dbConnection):

    def List_Request(self,my_rep_id = None):
        if my_rep_id:#for livecrawl
            my_rep_id = my_rep_id.replace(" ","")
            query = """SELECT * FROM requests_python where rep_id in %s"""
            arg = []
            for a in my_rep_id.split(","):
                arg.append(a)
            totalarg = [arg]
            self.db_curs.execute(query,totalarg)
            req = self.db_curs.fetchall()
        elif my_rep_id == None:
            query = """SELECT * FROM requests_python"""
            self.db_curs.execute(query)
            req = self.db_curs.fetchall()
        return req






class dbSites(dbConnection):
    pass





class dbZipcodes(dbConnection):
    def Load_Zipcodes(self):
        query = """SELECT zip_zipcode FROM zipcode"""
        self.db_curs.execute(query)
        Zipcodes_table = self.db_curs.fetchall()
        return Zipcodes_table