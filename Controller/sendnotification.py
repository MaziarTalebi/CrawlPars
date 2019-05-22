import smtplib
from email.message import EmailMessage
class SendNotification:
    
    def __init__(self):
        self.sender = "sm.talebi1@gmail.com"
        self.receiver = ["mt@projektgesellschaft.de","hh@projektgesellschaft.de"]
        #Ports 465 and 587 are intended for email client to email server communication - sending email
        self.server = smtplib.SMTP('smtp.gmail.com', 587)

        #starttls() is a way to take an existing insecure connection and upgrade it to a secure connection using SSL/TLS.
        self.server.starttls()

        #Next, log in to the server
        self.server.login("sm.talebi1@gmail.com", "nokia 5230 mahmoud")
    def missingAllCrawl(self,portal,crawltype,totalNumberOfZipcodeDone,totalNumberOfzipcodeToCrawl):
        body = "Unfortunately we are missing number of crawls for <<" + portal + ">> and crawltype " + str(crawltype) + " . For further information and details please check the log file."
        body2 = "total number of zipcode succesfully stored in database was::" + str(totalNumberOfZipcodeDone) + " out of total number of crawl::" + str(totalNumberOfzipcodeToCrawl)
        #Send the mail
        msg = EmailMessage()
        msg.set_content(body + body2)
        msg['Subject'] = "Heizol Report(Missing number of crawls)"
        msg['From'] = self.sender
        msg['To'] = ','.join(self.receiver)
        print(msg)
        # Send the message via our own SMTP server.
        try:
            self.server.sendmail(self.sender , self.receiver , msg.as_string())
        except Exception as e:
            self.server.quit()
            print(e)

#sendobj = SendNotification()
#sendobj.missingAllCrawl('1',1)