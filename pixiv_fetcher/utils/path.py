# -*- coding: utf-8 -*-
import os


def make_direct_open(path, *args, **kw):
    dirs, file_name = os.path.split(path)
    if not os.path.isdir(dirs):
        os.makedirs(dirs)
    mode = kw.get('mode', args[0] if args else None)
    if 'r' in mode and '+' in mode and not os.path.isfile(path):
        # r+模式下创建不存在的文件
        with open(path, 'w'):
            pass
    return open(path, *args, **kw)
