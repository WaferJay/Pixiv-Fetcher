import logging


def get_logger(obj, with_module=True):
    if with_module:
        name = '%s.%s' % (obj.__module__, obj.__class__.__name__)
    else:
        name = obj.__class__.__name__
    logger = logging.getLogger(name)
    return logger
