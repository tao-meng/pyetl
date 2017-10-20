from collections import Iterator
from sqlalchemy.types import String
from py_db import connection
import pandas
import functools
import logging
from py_etl.log import log
from py_etl.utils import run_time
from py_etl.task import TaskConfig


def upper(x):
    """
    输入中的元素转小写
    >>> to_lower('ABC')
    'abc'
    >>> to_lower(['ABc','CDE'])
    ['abc', 'cde']
    """
    if isinstance(x, list):
        return [i.upper() for i in x]
    else:
        return x.upper()


class Etl(object):
    _task_table = 'py_script_task'
    _query_size = 2000000
    _insert_size = 200000
    _field_default_size = 200
    _debug = False
    _symbol = ":1"

    @classmethod
    def config(cls, cfg):
        cls._symbol = getattr(cfg, 'SYMBOL', cls._symbol)
        cls._task_table = getattr(cfg, 'TASK_TABLE', cls._task_table)
        cls._query_size = getattr(cfg, 'QUERY_SIZE', cls._query_size)
        cls._insert_size = getattr(cfg, 'INSERT_SIZE', cls._insert_size)
        cls._field_default_size = getattr(
            cfg, 'NEW_TABLE_FIELD_DEFAULT_SIZE', cls._field_default_size)
        cls.src_uri = getattr(cfg, 'SRC_URI', None)
        cls.dst_uri = getattr(cfg, 'DST_URI', None)
        cls._debug = getattr(cfg, 'DEBUG', cls._debug)
        if cls._debug:
            log.setLevel(logging.DEBUG)

    def _connect(self, uri):
        if isinstance(uri, str):
            return connection(uri, debug=self._debug)
        else:
            args = [uri.pop('uri', ())]
            return connection(*args, **uri, debug=self._debug)

    def _create_obj(self, src_uri, dst_uri):
        src_uri = Etl.src_uri if src_uri is None else src_uri
        dst_uri = Etl.dst_uri if dst_uri is None else dst_uri
        return self._connect(src_uri), self._connect(dst_uri)

    def _handle_field(self):
        self.src_field = []
        self.src_field_name = []
        for i, j in self.mapping.items():
            if isinstance(j, list):
                self.src_field_name.extend(j)
                self.src_field.extend(
                    ["{} as {}{}".format(m, i, n) for n, m in enumerate(j)])
                self.mapping[i] = [
                    "{}{}".format(i, n) for n, m in enumerate(j)]
            else:
                self.src_field_name.append(j)
                self.src_field.append("{} as {}".format(j, i))
                self.mapping[i] = i
        if self.update:
            self.update_old = self.update
            if self.update not in self.src_field_name:
                self.src_field.append(self.update)
            else:
                self.update = [
                    i.split(" as ") for i in self.src_field if self.update in i
                ][0][1]

    def __init__(self, src_tab, dst_tab, mapping, update=None,
                 unique=None, src_uri=None, dst_uri=None):
        self.src_tab = src_tab
        self.dst_tab = dst_tab
        self.task = TaskConfig(self.src_tab, self.dst_tab)
        self.src_obj, self.dst_obj = self._create_obj(src_uri, dst_uri)
        self.mapping = {upper(i): upper(j) for i, j in mapping.items()}
        self.funs = {i: lambda x: x for i in self.mapping}

        if unique and update:
            if upper(unique) in self.mapping:
                self.unique = upper(unique)
            else:
                log.warn("unique：%s 名称错误" % self.unique)
                self.unique = None
        else:
            self.unique = None

        if update:
            self.update = upper(update)
            self.job, self.last_time = self.task.query()
        else:
            self.update = None
        self._handle_field()

    def add(self, col):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kw):
                return func(*args, **kw)
            self.funs[col.upper()] = func
            return wrapper
        return decorator

    def generate_sql(self, where, groupby):
        sql = ["select {columns} from {src_tab}".format(
            columns=','.join(self.src_field), src_tab=self.src_tab)]

        if self.last_time:
            sql.append("where %s>%s" % (self.update_old, self._symbol))
            args = [self.last_time]
            if where:
                sql.append('and (%s)' % where)
        else:
            args = []
            if where:
                sql.append('where (%s)' % where)
        if groupby:
            sql.append('group by')
            sql.append(groupby)
        sql = ' '.join(sql)
        return sql, args

    def generate_dataframe(self, sql, args):
        """
        获取源数据
        """
        df_iterator = pandas.read_sql(
            sql, self.src_obj.connect,
            params=args,
            chunksize=self._query_size)
        return df_iterator

    def _handle_data(self, src_df):
        if self.unique:
            df_sorted = src_df.sort_values(by=self.update, ascending=False)
            # print(df_sorted)
            src_df = df_sorted.drop_duplicates([self.unique])
            # df_drop = df_sorted.iloc[~df_sorted.index.isin(src_df.index)]
            # print(df_drop)
        data = {}
        for i, j in self.mapping.items():
            if isinstance(j, list):
                merge_arr = map(list, zip(*[src_df[x] for x in j]))
                data[i] = pandas.Series(merge_arr).map(self.funs[i])
            else:
                data[i] = src_df[i].map(self.funs[i])
        dst_df = pandas.DataFrame(data)
        return dst_df

    def run(self, where=None, groupby=None):
        sql, args = self.generate_sql(where, groupby)
        df_iterator = self.generate_dataframe(sql, args)
        for src_df in df_iterator:
            if self.update:
                last_time = src_df[self.update].max()
                self.last_time = max(self.last_time, last_time
                                     ) if self.last_time else last_time
            dst_df = self._handle_data(src_df)
            # if self._debug:
            #     dst_df.info()
            # print(dst_df)
            yield dst_df

    @run_time
    def join(self, *args, on=''):
        """
        合并数据入库
        """
        flag = None
        if len(args) == 1:
            for df in args[0]:
                flag = True
                self._to_save(df)
            else:
                log.info('没有数据更新') if flag is None else None
        else:
            args = map(next, args) if isinstance(args[0], Iterator) else args
            if len(args) == 2:
                rs = pandas.merge(*args, on=on, how='outer')
                self._to_save(rs)
            else:
                rs = pandas.merge(args[0], args[1], on=on, how='outer')
                new_args = (rs,) + args[2:]
                self.join(*new_args, on=on)

    def _to_save(self, df):
        """
        保存数据
        记录最后更新的时间点
        """
        if self.unique and self.last_time:
            columns = list(df.columns)
            print(df.values)
            args = [dict(zip(columns, i)) for i in df.values]
            self.dst_obj.merge(self.dst_tab, args, columns, self.unique)
        else:
            columns = list(df.columns)
            sql = "insert into %s(%s) values(%s)" % (
                self.dst_tab, ','.join(columns),
                ','.join(["%s" % self._symbol for i in range(len(columns))]))
            args = list(map(tuple, df.values))
            self.dst_obj.insert(sql, args)
            # df.to_sql(self.dst_tab, self.dst_obj.connect,
            #           dtype=String(self._field_default_size),
            #           if_exists='append', chunksize=self._insert_size,
            #           index=False)
        self.dst_obj.commit()
        log.info('插入数量：%s' % len(df))
        if self.last_time:
            if self.job:
                self.task.update(self.last_time.__str__())
            else:
                self.task.append(self.last_time.__str__())
        else:
            self.task.append()
