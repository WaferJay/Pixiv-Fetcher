

def datetime2gmt(dt):
    """
    :param dt:
    :type dt: datetime.datetime
    :return: str gmtime
    """
    return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')
