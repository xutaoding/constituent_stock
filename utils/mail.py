# -*- coding: utf-8 -*-
import email
import smtplib
from os.path import basename
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

USER = 'ops@chinascopefinancial.com'
PASSWORD = 'GmgW3UXF'
HOST = 'mail.chinascopefinancial.com'
PORT = 0


class Sender(object):
    def __init__(self, user=None, password=None, host=None, port=25,
                 receivers=None, subtype='mixed', timeout=60):
        # 默认邮箱服务器端口可能是25
        self.user = user or USER
        self.password = password or PASSWORD
        self.host = host or HOST
        self.port = PORT
        self.timeout = timeout
        self.receivers = receivers or []

        self.msg = MIMEMultipart(subtype)

    def connect(self):
        self.smtp = smtplib.SMTP(timeout=self.timeout)
        self.smtp.connect(self.host, self.port)
        self.smtp.login(self.user, self.password)

    def add_header(self, subject, priority=1):
        self.msg['From'] = Header(self.user)
        self.msg['To'] = Header(';'.join(self.receivers), 'utf-8')
        self.msg["Date"] = email.utils.formatdate(localtime=True)
        self.msg['Subject'] = Header('Subject: ' + subject, 'utf-8')
        self.msg['X-Priorit'] = Header(str(priority), 'utf-8')

    def send_email(self, subject, body, attaches=None):
        self.add_header(subject)
        smtp = smtplib.SMTP(timeout=self.timeout)
        smtp.connect(self.host, self.port)
        smtp.login(self.user, self.password)

        for attach in attaches or []:
            att = MIMEText(attach['attach_text'], 'base64', 'utf-8')
            att["Content-Type"] = 'application/octet-stream'
            att["Content-Disposition"] = 'attachment; filename="%s"' % attach['attach_name']
            self.msg.attach(att)

        self.msg.attach(MIMEText(body, 'plain', 'utf-8'))
        smtp.sendmail(self.user, self.receivers, self.msg.as_string())
        smtp.quit()


def send_email(sub, txt, receiver, copyto=None, priority="3"):
    #  chenglong.yan的写法
    msg = MIMEText(txt, _subtype="plain", _charset="utf-8")
    msg["Subject"] = sub
    msg["From"] = "ops@chinascopefinancial.com"
    msg["To"] = ";".join(receiver)
    msg["Date"] = email.utils.formatdate(localtime=True)

    if copyto:
        msg["Cc"] = ";".join(copyto)

    smtp = smtplib.SMTP()
    smtp.connect("mail.chinascopefinancial.com")
    smtp.login("ops@chinascopefinancial.com", "GmgW3UXF")
    msg.add_header("X-Priority", str(priority))
    smtp.sendmail("ops@chinascopefinancial.com", receiver, msg.as_string())
    smtp.close()


if __name__ == '__main__':
    # txt = [str(s) + '\n' for s in range(10)]
    # send_email('kakaka', ''.join(txt), ['xutao.ding@chinascopefinancial.com'])

    receiver = ['xutao.ding@chinascopefinancial.com', 'wenping.chen@chinascopefinancial.com']
    filename = 'D:/temp/indexes.txt'
    sender = Sender(receivers=receiver)
    sender.send_email(subject='TEST', body='akka test')
