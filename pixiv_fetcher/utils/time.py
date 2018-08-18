from __future__ import absolute_import

import time


def datetime2gmt(dt):
    """
    :param dt:
    :type dt: datetime.datetime
    :return: str gmtime
    """
    return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')


def datetime2timestamp(dt):
    """
    :param dt:
    :type dt: datetime.datetime
    :return: float timestamp
    """
    ts = time.mktime(dt.timetuple())
    ts += dt.microsecond / 1000000.0
    return ts
