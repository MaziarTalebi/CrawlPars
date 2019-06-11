
projectPATH = '/home/ubuntu/H_H/Crawl_Pars/'
import sys
if not projectPATH in sys.path:
	sys.path.append(projectPATH)
from conf import config
# import MySQLdb


class DBconnection:
    def __init__(self):
        self.host = config.DATABASE_CONFIG['host']
        self.port = config.DATABASE_CONFIG['port']
        self.user = config.DATABASE_CONFIG['user']
        self.DB = config.DATABASE_CONFIG['dbname']
        self.password = config.DATABASE_CONFIG['password']
        # print('Hello')
        # with open('/home/ubuntu/H_H/Crawl_Pars/Model/aaa.txt','w') as f:
        #     f.write(f'we are in init {self.host}  {self.port}  {self.user} lhgdskjhf ' )


    def __del__(self):
            self.db_curs.close()
            self.db_conn.close()
    def connectdb(self):
        self.db_conn=MySQLdb.connect(host="35.158.85.11", port=3306, user="mazi", passwd="mazi2crawler", db="crawler", use_unicode=True, charset='utf8')
        self.db_curs=self.db_conn.cursor()
        print('lsadkfgl')


# a = DBconnection()
# a.connectdb()
