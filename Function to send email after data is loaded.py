# function_to_send_email_for_load_reporting
##########################################
## Import packages
import email, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

## Function for sending email
def email_sender(mail_subject, mail_body, receiver_email_list):

  sender_email = 'abcd@xyz.com'
  smtp_host = "ndhsmtp.amer.company.com"
  smtp_port = "25"

## Create Multipart message and set headers
  msg = MIMEMultipart('alternative')
  msg['From'] = sender_email
  msg['To'] = receiver_email_list
  msg['Subject'] = mail_subject

## Add body to email and convert to string
  msg.attach(MIMEText(mail_body,'html'))
  smtp = smtplib.SMTP(smtp_host + ":" + smtp_port)
  smtp.starttls()
  smtp.sendmail(msg['From'],msg['To'].split(','),msg.as_string())

## Close smtp connection
  smtp.quit()

## Mail body
mail_body = '''\
    <html>
    <body>
    <p> Message that you want to send
    </body>
    </html>
    '''

## List of Recevier email ids
receiver_email_list = "abc@abc.com,def@abc.com"
mail_subject = "Subject text"

email_sender(mail_subject, mail_body, receiver_email_list)