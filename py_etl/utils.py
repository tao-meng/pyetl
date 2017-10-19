import json
import functools
import time
from py_etl.log import log


class ObjEncoder(json.JSONEncoder):
    def default(self, obj):
        return obj.__repr__()
        # if not isinstance(obj, (list)):
        #     return obj.__repr__()
        # return json.JSONEncoder.default(self, obj)


def reduce_num(n, l):
    num = min(n, l)
    if num > 10:
        return num % 10
    else:
        return 1


def run_time(func):
    """
    记录函数执行时间
    """
    @functools.wraps(func)
    def wrapper(*args, **kw):
        T = time.time()
        rs = func(*args, **kw)
        log.info('function [%s] run: %ss' % (
            func.__name__, round(time.time() - T, 3)))
        return rs
    return wrapper