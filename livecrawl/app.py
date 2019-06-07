from flask import Flask,request, render_template, url_for, redirect
from subprocess import Popen
from flask_ask import Ask, statement, question, session, context
import requests
import datetime
import MySQLdb
import os
app = Flask(__name__)
ask = Ask(app, "/")

# @app.before_request
# def limit_remote_addr():
#     return request.remote_addr
    # if request.remote_addr != '10.20.30.40':
    #     abort(403)  

@app.route('/generateUrl/')
def generateUrl():
    cmd = ['/usr/bin/python3', '/home/ubuntu/H_H/Crawl_Pars/fastenergy/fastenergy2/fastenergy.py']
    Popen(cmd)
    return 'generate url'

@app.route('/livecrawl/<req>')
def index(req):

    cmd_esyoil = ['/usr/bin/python3', '/home/ubuntu/H_H/Crawl_Pars/livecrawl/scrapytask_esyoil_live.py',req]
    Popen(cmd_esyoil)

    cmd_heizoel = ['/usr/bin/python3', '/home/ubuntu/H_H/Crawl_Pars/livecrawl/scrapytask_heizoel_live.py',req]
    Popen(cmd_heizoel)


    cmd_heizoel = ['/usr/bin/python3', '/home/ubuntu/H_H/Crawl_Pars/livecrawl/scrapytask_fastenergy_live.py',req]
    Popen(cmd_heizoel)

    return "okok"
@app.route("/output/<myvar>")
def output(myvar):
    with open('myvar.txt','w') as f:
        f.write(str(myvar))
    return "ok"
@app.route("/uploadip")
def fileFrontPage():
    return render_template('fileuploaderform.html')

@app.route("/handleUpload", methods=['POST'])
def handleFileUpload():
    uploadedfile = request.files['uploadingfile']
    fileNameInServer = ""
    if 'uploadingfile' in request.files:
        uploadedfile = request.files['uploadingfile']
        if uploadedfile.filename != '' and  uploadedfile.filename.count('.') == 1 and '.txt' in uploadedfile.filename:# if there is name for the file and it is txt format            
            fileNameInServer = str(datetime.datetime.now())  + uploadedfile.filename
            uploadedfile.save(os.path.join('/home/ubuntu/H_H/Crawl_Pars/files',fileNameInServer))
    try:
        import sys
        sys.path.append("../")
        from Services.UploadIPS import startUpgradingProxies
        if startUpgradingProxies('../files/' + fileNameInServer):
            return redirect(url_for('fileFrontPage'))
    except Exception as e:
        return e



@app.route('/alexacrawl/<zipcode>')
def startAlexaCrawl(zipcode):
    print(zipcode)
    ans = getBestPriceFromDB(zipcode)
    if ans == 10000:# no any crawl based on this zipcode
        cmd_esyoil = ['/usr/bin/python3', '/home/ubuntu/H_H/Crawl_Pars/livecrawl/scrapytask_esyoil_alexacrawl.py',zipcode]
        esyoilProcess = Popen(cmd_esyoil)
        cmd_esyoil = ['/usr/bin/python3', '/home/ubuntu/H_H/Crawl_Pars/livecrawl/scrapytask_heizoel_alexacrawl.py',zipcode]
        heizoelProcess = Popen(cmd_esyoil)
        while esyoilProcess.poll() == None:
            pass
        while heizoelProcess.poll() == None:
            pass
        ans = getBestPriceFromDB(zipcode)
    
    return str(ans)
    
def getBestPriceFromDB(zipcode):
    db_curs,db_conn = connection()
    query = """SELECT cra_price FROM crawls where cra_zipcode='((zipcode))' AND cra_crawl_type='5' AND cra_datetime > NOW() - INTERVAL 10 MINUTE"""
    query = query.replace("((zipcode))",zipcode)
    #print(query)
    db_curs.execute(query)
    IPS_table = db_curs.fetchall()
    #print(IPS_table)
    minPrice = selectMinimumPriceFromTuple(IPS_table)
    db_curs.close()
    db_conn.close()
    return minPrice
    
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
    
def selectMinimumPriceFromTuple(tuplePrice):
    minPrice = 10000
    for price in tuplePrice:
        tmp = list(price)[0]
        #print(tmp)
        if tmp < minPrice:
            minPrice = tmp
    return minPrice
        



@app.route('/')
def homepage():
    return "Hi there, how ya doin? all i fine???"


#@ask.launch
#def start_skill():
#    welcome_message = 'Halo'
#    return question(welcome_message)
#
#@ask.intent("SearchBestPricenew")
#def SearchBestPricenew():
#    headline_msg = 'Der beste Preis ist'
#    zipcode = get_alexa_location()
#    a = datetime.datetime.now()
#    startAlexaCrawl(zipcode)
#    diff = datetime.datetime.now() - a
#    headline_msg = headline_msg +' ' +str(diff.seconds)
##    before sending the message to alexa user we need to extract best price from DB based on the currenr session and zipcode
#    return statement(headline_msg)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port = 80,debug=True)
