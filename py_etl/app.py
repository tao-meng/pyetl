from collections import Iterator
from py_db import connection
import pandas
import sys
import logging
from py_etl.logger import log
from py_etl.utils import run_time, concat_place
from py_etl.task import TaskConfig


def upper(x):
    if isinstance(x, list):
        # return [i.upper() for i in x]
        return tuple([i.upper() for i in x])
    else:
        return x.upper()


def _change(func):
    def wrap(x):
        rs = func(*[i for i in x])
        return pandas.Series(list(rs))
    return wrap


class Etl(object):
    _src_place = ":1"
    _dst_place = ":1"
    _task_table = 'py_script_task'
    _query_count = 2000000
    _insert_count = 200000
    _field_size = 200
    _debug = False

    def config(self, cfg):
        self._src_place = getattr(cfg, 'SRC_PLACEHOLDER', self._src_place)
        self._dst_place = getattr(cfg, 'DST_PLACEHOLDER', self._dst_place)
        self._task_table = getattr(cfg, 'TASK_TABLE', self._task_table)
        self._query_count = getattr(cfg, 'QUERY_COUNT', self._query_count)
        self._insert_count = getattr(cfg, 'INSERT_COUNT', self._insert_count)
        self._field_size = getattr(
            cfg, 'CREATE_TABLE_FIELD_SIZE', self._field_size)
        self._debug = getattr(cfg, 'DEBUG', self._debug)
        if self._debug:
            log.setLevel(logging.DEBUG)
        self.src_uri = getattr(cfg, 'SRC_URI', None)
        self.dst_uri = getattr(cfg, 'DST_URI', None)
        if self.src_uri is None or self.dst_uri is None:
            log.error('没有配置数据库uri\nEXIT')
            sys.exit(1)
        self.src_obj, self.dst_obj = self._create_obj(
            self.src_uri, self.dst_uri)
        self.is_config = True

    def add(self, col):
        def decorator(func):
            self.funs[upper(col)] = func
        return decorator

    def _connect(self, uri):
        if isinstance(uri, str):
            return connection(uri, debug=self._debug)
        else:
            uri = uri.copy()
            args = [uri.pop('uri', ())]
            return connection(*args, **uri, debug=self._debug)

    def _create_obj(self, src_uri, dst_uri):
        # src_uri = Etl.src_uri if src_uri is None else src_uri
        # dst_uri = Etl.dst_uri if dst_uri is None else dst_uri
        return self._connect(src_uri), self._connect(dst_uri)

    def __init__(self, src_tab, dst_tab, mapping, update=None, unique=None):
        self.src_tab = src_tab
        self.dst_tab = dst_tab
        self.is_config = False
        self.mapping = {upper(i): upper(j) for i, j in mapping.items()}
        # self.funs = {i: lambda x: x for i in self.mapping}
        self.funs = {}
        if unique and update:
            if upper(unique) in self.mapping:
                self.unique = upper(unique)
            else:
                log.error("unique：%s 名称错误\nEXIT" % self.unique)
                sys.exit(1)
        else:
            self.unique = None
        if update:
            self.update = upper(update)
        else:
            self.update = None

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
                self.src_field.append("%s as etl_update_flag" % self.update)
                self.update = "etl_update_flag".upper()
            else:
                self.update = [
                    i.split(" as ") for i in self.src_field if self.update in i
                ][0][1]

    def generate_sql(self, where, groupby, days):
        self.task = TaskConfig(self.src_tab, self.dst_tab)
        if self.update:
            self.job, self.last_time = self.task.query(days)
        else:
            self.job, self.last_time = None, None
        self._handle_field()
        sql = ["select {columns} from {src_tab}".format(
            columns=','.join(self.src_field), src_tab=self.src_tab)]
        args = []
        if self.update:
            sql.append("where %s is not null" % self.update_old)
            if self.last_time:
                sql.append("and %s>%s" % (self.update_old, self._src_place))
                args = [self.last_time]
            if where:
                sql.append('and (%s)' % where)
        else:
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
            chunksize=self._query_count)
        return df_iterator

    def _handle_data(self, src_df):
        if self.unique:
            df_sorted = src_df.sort_values(by=self.update, ascending=False)
            src_df = df_sorted.drop_duplicates([self.unique])
            # df_drop = df_sorted.iloc[~df_sorted.index.isin(src_df.index)]
            # print(df_drop)
        if len(src_df) > 5:
            log.debug("src data\n%s\n\t\t\t\t......" % src_df[:5])
        else:
            log.debug("src data\n%s" % src_df[:5])
        data = {}
        for i, j in self.mapping.items():
            if isinstance(j, list):
                merge_arr = map(list, zip(*[src_df[x] for x in j]))
                if i in self.funs:
                    data[i] = pandas.Series(merge_arr).map(self.funs[i])
                    self.funs.pop(i)
                else:
                    log.error("'{}'字段是一个列表需要添加处理函数\nEXIT".format(i))
                    sys.exit(1)
            else:
                if i in self.funs:
                    data[i] = src_df[i].map(self.funs[i])
                    self.funs.pop(i)
                else:
                    data[i] = src_df[i]
        for i in self.funs:
            cols = list(i)
            if (isinstance(i, tuple) and
                    (set(cols) & set(self.mapping.keys()) == set(cols))):
                tmp_df = src_df[cols].apply(_change(self.funs[i]), axis=1)
                # print(src_df[cols].dtypes, tmp_df.dtypes)
                for idx, col in enumerate(cols):
                    data[col] = tmp_df[idx]
            else:
                log.error("所添函数{}字段与实际字段不匹配\nEXIT".format(i))
                sys.exit(1)
        dst_df = pandas.DataFrame(data)[list(self.mapping.keys())]
        return dst_df

    def run(self, where=None, groupby=None, days=None):
        if not self.is_config:
            log.error('需要先加载配置文件\nEXIT')
            sys.exit(1)
        sql, args = self.generate_sql(where, groupby, days)
        log.debug("%s, Param: %s" % (sql, args))
        df_iterator = self.generate_dataframe(sql, args)
        for src_df in df_iterator:
            column_upper = [i.upper() for i in list(src_df.columns)]
            src_df.rename(
                columns={i: j for i, j in zip(src_df.columns, column_upper)},
                inplace=True)
            # log.debug("src data\n%s\n..." % src_df[:5])
            if self.update:
                # print((src_df[self.update].dtypes))
                # src_df[self.update] = src_df[self.update].astype(
                #     'datetime64[ns]')
                # print((src_df[self.update].dtypes))
                last_time = src_df[self.update].max()
                self.last_time = max(self.last_time, last_time
                                     ) if self.last_time else last_time
            dst_df = self._handle_data(src_df)
            # dst_df.info()
            if len(dst_df) > 5:
                log.debug("dst data\n%s\n\t\t\t\t......" % dst_df[:5])
            else:
                log.debug("dst data\n%s" % dst_df[:5])
            # log.debug("dst data\n%s" % dst_df)
            yield dst_df

    @run_time
    def save(self, *args, on=''):
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
            if not on:
                log.error("join多个dataframe需要参数on \n"
                          "example job.join(rs1, rs2, rs3, on=['id'])"
                          "\nEXIT")
                sys.exit()
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
        columns = list(df.columns)
        sql = "insert into %s(%s) values(%s)" % (
            self.dst_tab, ','.join(columns),
            concat_place(columns, place=self._dst_place))
        if self.last_time:
            if self.job:
                args = [dict(zip(columns, i)) for i in df.values]
                self.dst_obj.merge(self.dst_tab, args, columns, self.unique)
                self.task.update(self.last_time)
            else:
                args = list(map(tuple, df.values))
                self.dst_obj.insert(sql, args, num=self._insert_count)
                self.task.append(self.last_time)
        else:
            args = list(map(tuple, df.values))
            self.dst_obj.insert(sql, args, num=self._insert_count)
            self.task.append()
        self.dst_obj.commit()
        log.info('插入数量：%s' % len(df))
