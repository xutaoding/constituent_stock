# -*- coding: utf-8 -*-
import ftplib
import os
import re
import sys
import zipfile

import pandas

reload(sys)
sys.setdefaultencoding("utf8")


class Ftp(ftplib.FTP):
    def __init__(self, uri, user="", passwd=""):
        splits = uri.split("/")
        ip = splits[0]
        self.uri = uri
        self.user = user
        self.passwd = passwd
        self.ip = ip

        ftplib.FTP.__init__(self, ip)
        self.login(user, passwd)

        if len(splits) > 1:
            self.cwd("/".join(splits[1:]))

    def __del__(self):
        self.close()

    def reconnect(self):
        self.connect(self.ip)
        self.login(user=self.user, passwd=self.passwd)

    def list(self, reg=".+"):
        reg = re.compile(reg)
        return [n for n in self.nlst() if reg.findall(n)]

    def download(self, file, save_path="."):
        file_path = os.sep.join([save_path, file])

        if not os.path.exists(save_path):
            os.mkdir(save_path)

        with open(file_path, "wb") as cache:
            try:
                self.retrbinary("RETR %s" % file, cache.write)
            except:
                return pandas.DataFrame()

        if not zipfile.is_zipfile(file_path):
            return pandas.read_excel(file_path)

        with zipfile.ZipFile(file_path, "r") as zip:
            result = pandas.concat([pandas.read_excel(zip.open(name)) for name in zip.namelist()])

            return result.drop_duplicates()
