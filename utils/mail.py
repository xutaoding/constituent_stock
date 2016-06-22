import email
import smtplib
from email.mime.text import MIMEText


def send_email(sub, txt, receiver, copyto=None, priority="3"):
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
    txt = [str(s) + '\n' for s in range(10)]
    send_email('kakaka', ''.join(txt), ['xutao.ding@chinascopefinancial.com'])
