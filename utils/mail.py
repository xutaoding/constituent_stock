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
        self.receivers = ';'.join(receivers or [])

        self.msg = MIMEMultipart(subtype)

    def connect(self):
        self.smtp = smtplib.SMTP(timeout=self.timeout)
        self.smtp.connect(self.host, self.port)
        self.smtp.login(self.user, self.password)

    def add_header(self, subject):
        self.msg['From'] = Header(self.user, 'utf-8')
        self.msg['To'] = Header(self.receivers, 'utf-8')
        self.msg["Date"] = email.utils.formatdate(localtime=True)
        self.msg['Subject'] = Header('Subject: ' + subject, 'utf-8')

    def _add_attach(self, attach_content, filename):
        # 构造附件
        att = MIMEText(attach_content, 'base64', 'utf-8')
        att["Content-Type"] = "application/octet-stream"
        att["Content-Disposition"] = 'attachment; filename="%s"' % basename(filename)
        self.msg.attach(att)

    def _attach_files(self, files=None):
        files = files or []
        file_names = files if isinstance(files, (list, tuple)) else [files]

        for _filename in file_names:
            with open(filename) as fp:
                self._add_attach(fp.read(), _filename)

    def _alone_attaches(self, alone_attaches=None):
        alone_attaches = alone_attaches or []
        attaches = alone_attaches if isinstance(alone_attaches, (list, tuple)) else [alone_attaches]

        for attach in attaches:
            attach_text = attach.get('attach_text', '')
            attach_name = attach.get('attach_name', 'text.txt')
            self._add_attach(attach_text, attach_name)

    def add_attach(self, mail_body, files=None, alone_attaches=None):
        self._attach_files(files)
        self._alone_attaches(alone_attaches)

        # Show mail body text
        content = MIMEText(mail_body, 'plain', 'utf-8')
        self.msg.attach(content)

    def send_email(self, subject='Just test', mail_body='Test', files=None, alone_attaches=None,
                   mail_options=None, rcpt_options=None):
        # 发送邮件或发送文本
        mail_options = mail_options or []
        rcpt_options = rcpt_options or []

        try:
            self.connect()
            self.add_header(subject)
            self.add_attach(mail_body, files, alone_attaches)
            self.smtp.sendmail(self.user, self.receivers, self.msg.as_string(), mail_options, rcpt_options)
        except Exception:
            pass
        finally:
            self.smtp.quit()


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

    receiver = ['xutao.ding@chinascopefinancial.com']
    filename = 'D:/temp/indexes.txt'
    sender = Sender(receivers=receiver)
    # sender.send_email(files=filename)
    sender.send_email(
        alone_attaches=[{'attach_name': 'gggg.txt', 'attach_text': '\n'.join([str(s) for s in range(10)])}]
    )
