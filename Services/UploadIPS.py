import MySQLdb
import datetime

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

def deleteIPs():
    db_curs,db_conn = connection()
    query = "DELETE FROM ips"
    db_curs.execute(query)
    db_conn.commit()
    db_curs.close()
    db_conn.close()

def insertListIPs(listips):
    db_curs,db_conn = connection()
    for ip in listips:
        db_curs.execute("""INSERT INTO ips VALUES (%s,%s,%s,%s,%s)""",(ip.replace('\n',''),'1','10', datetime.datetime.now() , datetime.datetime.now()))
    db_conn.commit()
    db_curs.close()
    db_conn.close()
def startUpgradingProxies(filename):#file name should include path also
    listOfIPs = []
    with open(filename , 'r') as f:
        listOfIPs = f.readlines()
    # Delete all the previous IPs
    deleteIPs()
    insertListIPs(listOfIPs)
    return True