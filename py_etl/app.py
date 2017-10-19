from collections import Iterator
from sqlalchemy.types import String
from py_db import connection
import pandas
import functools
import logging
from py_etl.utils import run_time
from py_etl.log import log
from py_etl.utils import taskUtil


def lower(x):
    """
    输入中的元素转小写
    >>> to_lower('ABC')
    'abc'
    >>> to_lower(['ABc','CDE'])
    ['abc', 'cde']
    """
    if isinstance(x, list):
        return [i.lower() for i in x]
    else:
        return x.lower()


class Etl(object):
    _task_table = 'py_script_task'
    _query_size = 2000000
    _insert_size = 200000
    _field_default_size = 200
    _print_debug = False
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
            args = uri.pop('uri', ())
            return connection(*args, **uri, debug=self._debug)

    def _create_obj(self, src_uri, dst_uri):
        src_uri = Etl.src_uri if src_uri is None else None
        dst_uri = Etl.dst_uri if dst_uri is None else None
        return self._connect(src_uri), self._connect(dst_uri)

    def _handle_field(self):
        self.src_field = []
        self.src_field_name = []
        # self.new_map = {}
        for i, j in self.mapping.items():
            if isinstance(j, list):
                self.src_field_name.extend(j)
                self.src_field.extend(
                    ["{} as {}{}".format(m, i, n) for n, m in enumerate(j)])
                self.mapping[i] = ["{}{}".format(i, n) for n, m in enumerate(j)]
            else:
                self.src_field_name.append(j)
                self.src_field.append("{} as {}".format(j, i))
                self.mapping[i] = i
        if self.update and self.update not in self.src_field_name:
                self.src_field.append(self.update)

    def __init__(self, src_tab, dst_tab, mapping, update=None,
                 unique=None, src_uri=None, dst_uri=None):
        self.src_tab = src_tab
        self.dst_tab = dst_tab
        self.task = taskUtil(self.src_tab, self.dst_tab)
        # self.src_tab = src_tab.lower()
        # self.dst_tab = dst_tab.lower()
        self.src_obj, self.dst_obj = self._create_obj(src_uri, dst_uri)
        self.mapping = {lower(i): lower(j) for i, j in mapping.items()}
        self.funs = {i: lambda x: x for i in self.mapping}

        if unique and update:
            # self.unique = self.mapping.get(lower(unique), None)
            self.unique = lower(unique)
        else:
            self.unique = None

        if update:
            self.update = lower(update)
            self.job, self.last_time = self.task.query()[0]
        else:
            self.update = None
        self._handle_field()

    def add(self, col):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kw):
                return func(*args, **kw)
            self.funs[col.lower()] = func
            return wrapper
        return decorator

    # @classmethod
    # def query(cls, sql, engine=None):
    #     """
    #     sql查询操作
    #     """
    #     if engine is None:
    #         engine = cls.dst_db_uri
    #     with connection(engine) as db:
    #         log.info(engine)
    #         res = db.query(sql)
    #     return res

    # def _query_data_generator(self, sql, args):
    #     with connection(self.src_engine) as db:
    #         res = db.query_dict(sql, args, chunksize=self.__query_size)
    #         for r in res:
    #             data = {}
    #             for i, d in r:
    #                 data[i] = pandas.Series(d)
    #             rs = pandas.DataFrame(data)
    #             yield rs

    # def query_data(self, sql, args):
    #     if isinstance(self.src_engine, engine.base.Engine):
    #         iter_df = pandas.io.sql.read_sql_query(
    #             sql, self.src_engine,
    #             params=args,
    #             chunksize=self.__query_size)
    #         return iter_df
    #     else:
    #         return self._query_data_generator(sql, args)

    def generate_sql(self, where, groupby):
        sql = ["select {columns} from {src_tab}".format(
            columns=','.join(self.src_field), src_tab=self.src_tab)]

        if self.last_time:
            sql.append("where %s>%s" % (self.update, self._symbol))
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
        # for src_df in df_iterator:
        #     yield src_df

    def run(self, where=None, groupby=None):
        df_iterator = self.generate_dataframe(*self.generate_sql(where, groupby))
        for src_df in df_iterator:
            if self.update:
                last_time = src_df[self.update].max()
                self.last_time = max(self.last_time, last_time) if self.last_time else last_time
            if self.unique:
                src_df = src_df.sort_index(by=self.update, ascending=False)
                # print(src_df.duplicated())
                src_df = src_df.drop_duplicates([self.unique])
            data = {}
            for i, j in self.mapping.items():
                if isinstance(j, list):
                    merge_colume = map(list, zip(*[src_df[x] for x in j]))
                    data[i] = pandas.Series(merge_colume).map(self.funs[i])
                else:
                    data[i] = src_df[j].map(self.funs[i])
            dst_df = pandas.DataFrame(data)
            # if self.__print_debug:
            #     print('dst table info')
            #     dst_df.info()
            print(dst_df[:4])
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
                self.to_save(df)
            else:
                log.info('没有数据更新') if flag is None else None
        else:
            args = map(next, args) if isinstance(args[0], Iterator) else args
            if len(args) == 2:
                rs = pandas.merge(*args, on=on, how='outer')
                self.to_save(rs)
            else:
                rs = pandas.merge(args[0], args[1], on=on, how='outer')
                new_args = (rs,) + args[2:]
                self.join(*new_args, on=on)

    def to_save(self, df):
        """
        保存数据
        记录最后更新的时间点
        """
        if self.unique and self.last_time:
            # df.to_dict(orient='dict')
            columns = list(df.columns)
            args = [dict(zip(columns, i)) for i in df.values]
            self.dst_obj.merge(self.dst_table, args, columns, self.dst_unique)
        else:
            df.to_sql(self.dst_tab.lower(), self.dst_obj.connect,
                      dtype=String(self._field_default_size),
                      if_exists='append', chunksize=self.__insert_size, index=False)
        log.info('插入数量：%s' % len(df))
        if self.last_time:
            if self.job:
                self.task.update(self.last_time.__str__())
            else:
                self.task.append(self.last_time.__str__())
        else:
            self.task.append()
