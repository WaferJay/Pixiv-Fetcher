import datetime
import re
from collections import namedtuple

_p_pximg = re.compile('/(?P<datetime>\d{4}/(?:\d{2}/){5})'
                      '(?P<id>\d+)_p(?P<page>\d+)(?:_master1200)?'
                      '\.(?P<extension>jpg|png|gif|jpeg)$', re.I)

_datetime_format = '%Y/%m/%d/%H/%M/%S/'

PixivImage = namedtuple('PixivImage', ['pid', 'page', 'extension', 'datetime'])


def parse_pximg_url(url):
    match = _p_pximg.search(url)
    if match:
        results = match.groupdict()
        dt_str = results['datetime']
        dt = datetime.datetime.strptime(dt_str, _datetime_format)
        extension = results['extension']
        pid = int(results['id'])
        page = int(results['page'])

        return PixivImage(pid, page, extension, dt)

    return None
