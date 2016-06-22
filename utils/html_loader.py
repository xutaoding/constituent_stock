import time
import urllib2
import urllib
import requests
from random import choice

from conf import USER_AGENT
from conf import logger as _logger
from conf.indexes import REQUIRED_INDEXES as _RINDEX


class HtmlLoader(object):
    logger = _logger

    def get_html(self, url, headers=None, cookies=None, **kwargs):
        required_cookie = cookies
        required_headers = headers or {'User-Agent': choice(USER_AGENT)}
        for _ in range(3):
            try:
                response = requests.get(url, headers=required_headers, cookies=required_cookie, **kwargs).content
                return response
            except Exception as e:
                self.logger.info("Get html error: type <{}>, msg <{}>".format(e.__class__, e))
        return ''

    def get_raw_html(self, url, data=None, **kwargs):
        for i in range(1, 4):
            req = urllib2.Request(url) if not data else urllib2.Request(url, urllib.urlencode(data))
            req.add_header('User-Agent', choice(USER_AGENT))

            for head_value in kwargs.itervalues():
                for key, value in head_value.iteritems():
                    req.add_header(key, value)

            try:
                response = urllib2.urlopen(req, timeout=30)
                feed_data = response.read()
                response.close()
                return feed_data
            except Exception as e:
                self.logger.info('Web open error: type <{}>, msg <{}>, time <{}>'.format(e.__class__, e, i))
                time.sleep(3)
        return '<html></html>'

    @staticmethod
    def validate_index(p_code):
        if p_code not in _RINDEX:
            return False
        return True
