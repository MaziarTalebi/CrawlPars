# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import MySQLdb.cursors
import hashlib
import sys
import datetime





class Esyoil2Pipeline(object):
    def connection(self):
        conn=MySQLdb.connect(host="35.158.85.11",
                             port=3306,
                             user="mazi",
                             passwd="mazi2crawler",
                             db="crawler",
                             use_unicode=True,
                             charset='utf8')
        c=conn.cursor()
        return c,conn
    def Load_Dealer(self,company_name,siteid):
        db_curs,db_conn = self.connection()
        try:
            result = -1
            query = """SELECT dea_id,dea_name FROM dealer where dea_fk_sit='(/siteid/)'"""
            query = query.replace('(/siteid/)',str(siteid))
            print("query is",query)
            db_curs.execute(query)
            result_table=db_curs.fetchall()
            for my_id,my_name in result_table:
                if my_name.replace("\xa0"," ") == company_name:
                    result = my_id
                    break
        except Exception as e:
            result = -1000
            print("<problem>error in Load_Dealer",e)
        finally:
            db_curs.close()
            db_conn.close()
        return result

    def Update_Dealer(self,company_name,site_id):
        try:
            db_curs,db_conn = self.connection()
            db_curs.execute("""INSERT INTO dealer VALUES(%s,%s,%s,%s)""",('0',company_name,datetime.datetime.now(),str(site_id)))
            db_conn.commit()
        except Exception as e:
            print("<problem>error in Update_Dealer",e)
        finally:
            db_curs.close()
            db_conn.close()

    def MD5_HASH(self,md5str):
        md5obj=hashlib.md5()
        md5obj.update(md5str)
        return md5obj.hexdigest()
    def Insert_esyoil(self,item):
        if int(float(item['price'])) > 0:
            try:
                fk_dealer = self.Load_Dealer(item['providername'] , item['esyoild_site_id'])
                if fk_dealer == -1 :
                    print("couldnt find ::",item['providername'],"we will update now")
                    self.Update_Dealer(item['providername'],item['esyoild_site_id'])
                    fk_dealer = self.Load_Dealer(item['providername'] , item['esyoild_site_id'])
                    print("we did update and get the id ::",fk_dealer)
                    item['fk_dealer'] = fk_dealer
                elif fk_dealer >= 0:
                    print("only one found for ::",item['providername'])
                    print(fk_dealer)
                    item['fk_dealer'] = fk_dealer
                elif fk_dealer == -1000:
                    print("<problem> there is sth wrong in load dealer")
                md5_str = str(item['zipcode'])+'-'+str(item['cra_amount'])+'-'+str(item['up'])+'-'+str(item['cra_pro'])
                print(md5_str)
                # item['cra_hash'] = self.MD5_HASH(md5_str.encode('utf-8'))
                if item['cra_pro'] == 8:
                    item['cra_pro'] = 1
                if item['cra_pro'] == 4:
                    item['cra_pro'] = 2
                db_curs,db_conn = self.connection()
                db_curs.execute("""INSERT INTO crawls VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                             ('0',item['cra_hash'],item['datetime'],item['esyoild_site_id'],item['fk_ans'],item['zipcode'],item['cra_amount'],item['up'],item['cra_pro'],item['payment_type'],item['cra_express'],item['cra_hose'],item['cra_short_vehicle'],item['providername'],item['deliverydate'],item['cra_position'],item['price'],item['Rate'],item['Reviews'],item['fk_dealer'],item['crawl_type']))
                print("Greate these data has been successfully inserted::",datetime.datetime.now(),item['cra_hash'],item['datetime'],item['esyoild_site_id'],item['fk_ans'],item['zipcode'],item['cra_amount'],item['up'],item['cra_pro'],item['payment_type'],item['cra_express'],item['cra_hose'],item['cra_short_vehicle'],item['providername'],item['deliverydate'],item['cra_position'],item['price'],item['Rate'],item['Reviews'],item['fk_dealer'],item['crawl_type'])
                db_conn.commit()
            except (MySQLdb.Error, MySQLdb.Warning) as e:
                print ("<problem>Time of the exception ERROR",datetime.datetime.now(),e)
    
            finally:
                print ("<<<<<<<<<<<<< Here is FINALLY  >>>>>>>>>>")
                db_curs.close()
                db_conn.close()
    def process_item(self, item, spider):
        print("Insertion to DB start for all the dealer for crawl::")
        for it in item["alllist"]:
            self.Insert_esyoil(it)
        print("Insertion to DB over for all the dealer for crawl::")
        # return item