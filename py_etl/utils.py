import json
import functools
import time
import datetime
from dateutil import rrule, parser
from dateutil.relativedelta import relativedelta
if __name__ == "__main__":
    from logger import log
else:
    from py_etl.logger import log


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
        log.info('function [%s] run: %ss\n' % (
            func.__name__, round(time.time() - T, 3)))
        return rs
    return wrapper


def concat_field(field, concat=','):
    """
    >>> concat_field(['id','name'])
    'id,name'
    """
    return concat.join(field)


def concat_place(field, place=":1"):
    """
    >>> concat_place(['id','name'])
    ':1,:1'
    """
    return ','.join([place for i in range(len(field))])


class DateUtility(object):

    def dates_list(self, start, end):
        """
        生成日期数组
        """
        rs = rrule.rrule(
            rrule.DAILY, dtstart=parser.parse(start), until=parser.parse(end))
        return list(rs)

    def dates_list_str(self, start, end):
        """
        生成日期数组字符串
        """
        dates = []
        for date_str in sorted(self.date_times(start, end)):
            dates.append(date_str.strftime("%Y%m%d"))
        return dates

    def delta_days(self, start, end):
        """
        相差多少天
        >>> dtutil.delta_date('20170930', '20171001')
        1
        """
        start = parser.parse(start)
        end = parser.parse(end)
        return (end - start).days

    def days_ago_str(self, date=datetime.date.today(), delta=None):
        """
        >>> dtutil.days_ago_str(days=1)
        '20171024'
        """
        if delta:
            result_date = parser.parse(str(date)) - datetime.timedelta(days=delta)
            return result_date.strftime('%Y%m%d')
        else:
            return date

    def minutes_ago(self, date=datetime.datetime.now(), delta=0):
        result_date = parser.parse(str(date)) + datetime.timedelta(minutes=-delta)
        # result_date.strftime("%Y%m%d%H%M%S")
        return result_date

    def hours_ago(self, date=datetime.datetime.now(), delta=0):
        result_date = parser.parse(str(date)) + datetime.timedelta(hours=-delta)
        return result_date

    def days_ago(self, date=datetime.datetime.now(), delta=0):
        result_date = parser.parse(str(date)) + datetime.timedelta(days=-delta)
        return result_date

    def month_ago(self, date=datetime.datetime.now(), delta=0):
        result_date = parser.parse(str(date)) + relativedelta(months=-delta)
        return result_date


dtutil = DateUtility()


if __name__ == "__main__":
    now = datetime.datetime.now()
    rs = dtutil.delta_date('20170930', '20171001')
    rs2 = dtutil.days_ago()
    print(rs, rs2.__repr__())
