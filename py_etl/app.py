from collections import Iterator
from py_db import connection
# from collections import defaultdict
import pandas
import sys
import logging
from py_etl.logger import log
from py_etl.utils import run_time, concat_place
from py_etl.task import TaskConfig
# pandas.set_option('display.height', 1000)
# pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)


def upper(x):
    if isinstance(x, list):
        return tuple([i.upper() for i in x])
    else:
        return x.upper()


def _change(func):
    def wrap(x):
        try:
            rs = func(*[i for i in x])
            return pandas.Series(list(rs))
        except Exception as r:
            log.error('handle fun fail input: %s, output: %s' % (x, rs))
            raise r
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
        self.db = self.dst_obj
        self.is_config = True

    def add(self, col):
        def decorator(func):
            self.funs[upper(col)] = func
        return decorator

    def _connect(self, uri):
        if isinstance(uri, str):
            return connection(uri, debug=self._debug)
        elif isinstance(uri, dict):
            uri = uri.copy()
            if 'file' in uri:
                if self.update:
                    log.error('处理文件中数据时，update参数必须为空')
                    sys.exit(1)
                else:
                    return uri['file']
            else:
                debug = uri.get("debug", self._debug)
                uri.pop("debug", None)
                return connection(**uri, debug=debug)
        else:
            log.error("无效的数据库配置(%s)" % uri.__repr__())
            sys.exit(1)

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
            # self.unique = None 2.0.4 版本改动
            self.unique = upper(unique) if unique else None
        if update:
            self.update = upper(update)
        else:
            self.update = None

    def _handle_field(self):
        """
        字段名称处理
        """
        self.src_field = []
        self.src_field_name = []
        self.src_field_dict = {}
        for i, j in self.mapping.items():
            if isinstance(j, (list, tuple)):
                self.src_field_name.extend(j)
                self.src_field_dict.update(
                    {m: "{}{}".format(i, n) for n, m in enumerate(j)})
                self.src_field.extend(
                    ["{} as {}{}".format(m, i, n) for n, m in enumerate(j)])
                self.mapping[i] = [
                    "{}{}".format(i, n) for n, m in enumerate(j)]
            else:
                self.src_field_name.append(j)
                self.src_field_dict[j] = i
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

    def generate_sql(self, where, groupby):
        """
        生成数据查询sql
        """
        # self._check_task(days)
        # self._handle_field()
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

    def _handle_data(self, src_df):
        """
        数据处理
        Args:
            src_df: dataframe
        return:
            dataframe
        """
        if len(src_df) > 5:
            log.debug("src data\n%s\n\t\t\t\t......" % src_df[:5])
        else:
            log.debug("src data\n%s" % src_df[:5])
        log.debug("src type:\n%s" % pandas.DataFrame(src_df.dtypes).T)
        data = {}
        for i, j in self.mapping.items():
            if isinstance(j, (list, tuple)):
                merge_arr = map(list, zip(*[src_df[x] for x in j]))
                # merge_arr = list(zip(*[src_df[x].values for x in j]))
                if i in self.funs:
                    data[i] = pandas.Series(merge_arr).map(self.funs[i])
                    # data[i] = pandas.Series((map(self.funs[i], merge_arr)))
                    self.funs.pop(i)
                else:
                    log.error("'{}'字段是一个列表需要添加处理函数\nEXIT".format(i))
                    sys.exit(1)
            else:
                if i in self.funs:
                    data[i] = src_df[i].map(self.funs[i])
                    # data[i] = pandas.Series(map(self.funs[i], src_df[i].values))
                    self.funs.pop(i)
                else:
                    data[i] = src_df[i]
        for i in self.funs:
            cols = list(i)
            if (isinstance(i, (tuple, list)) and
                    (set(cols) & set(self.mapping.keys()) == set(cols))):
                tmp_df = src_df[cols].apply(_change(self.funs[i]), axis=1)
                for idx, col in enumerate(cols):
                    data[col] = tmp_df[idx]
                # tmp = defaultdict(list)
                # merge_arr = list(zip(*[src_df[x].values for x in cols]))
                # for ele in map(self.funs[i], merge_arr):
                #     for idx, col in enumerate(cols):
                #         tmp[col].append(ele[idx])
                # for col in cols:
                #     data[col] = pandas.Series(tmp[col])
            else:
                log.error("所添函数'{}'字段与实际字段{}不匹配\nEXIT".format(i, self.mapping.keys()))
                sys.exit(1)
        if self.unique:
            if self.update:
                data[self.update] = src_df[self.update]
                tmp_df = pandas.DataFrame(data)
                df_sorted = tmp_df.sort_values(by=self.update, ascending=False)
            else:
                df_sorted = pandas.DataFrame(data)
            tmp_df = df_sorted.drop_duplicates([self.unique])
            # df_drop = df_sorted.iloc[~df_sorted.index.isin(src_df.index)]
        else:
            tmp_df = pandas.DataFrame(data)
        dst_df = tmp_df[list(self.mapping.keys())]
        return dst_df

    def _check_task(self, days):
        self.task = TaskConfig(self.src_tab, self.dst_tab)
        # self.task = TaskConfig(self.src_tab, self.dst_tab, self._debug)
        if self.update:
            self.job, self.last_time = self.task.query(days)
        else:
            self.job, self.last_time = None, None
        self.last = self.last_time

    def generate_dataframe(self, where, groupby, days):
        """
        获取源数据
        """
        self._check_task(days)
        self._handle_field()
        if isinstance(self.src_obj, str):
            if self.src_tab=='csv':
                df_iterator = pandas.read_csv(
                    self.src_obj,
                    chunksize=self._query_count)
            else:
                log.error("文件类型'{}'不支持".format(self.src_tab))
                sys.exit()
        else:
            sql, args = self.generate_sql(where, groupby)
            log.debug("%s, Param: %s" % (sql, args))
            df_iterator = pandas.read_sql(
                sql, self.src_obj.connect,
                params=args,
                chunksize=self._query_count)
        return df_iterator

    def run(self, where=None, groupby=None, days=None):
        """
        数据处理任务执行
        args:
            where: 查询sql的过滤条件 example: where="id is not null"
            groupby: 查询sql的group by 语句
            days: 为0 表示全量重跑， 其他数字表示重新查询days天前的数据
        return:
            处理完成的数据 (generator object)
        """
        if not self.is_config:
            log.error('需要先加载配置文件\nEXIT')
            sys.exit(1)
        df_iterator = self.generate_dataframe(where, groupby, days)
        for src_df in df_iterator:
            # 替换 NaN 为None
            # src_df = src_df.where(src_df.notnull(), None)
            if isinstance(self.src_obj, str):
                column_upper = [self.src_field_dict[i].upper() for i in list(src_df.columns)]
            else:
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
            log.debug("dst type:\n%s" % pandas.DataFrame(dst_df.dtypes).T)
            if len(dst_df) > 5:
                log.debug("dst data\n%s\n\t\t\t\t......" % dst_df[:5])
            else:
                log.debug("dst data\n%s" % dst_df[:5])
            # 替换 NaN 为None
            dst_df = dst_df.where(dst_df.notnull(), None)
            yield dst_df

    @run_time
    def save(self, *args, on=''):
        """
        处理后的数据入库
        args:
            args:接收数据，格式是Iterable object
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
        if isinstance(self.dst_obj, str):
            if self.dst_tab=='csv':
                df.to_csv(self.dst_obj, index=False)
            else:
                log.error("文件类型'{}'不支持".format(self.dst_tab))
                sys.exit()
        else:
            columns = list(df.columns)
            sql = "insert into %s(%s) values(%s)" % (
                self.dst_tab, ','.join(columns),
                concat_place(columns, place=self._dst_place))
            if self.last_time:
                if self.job:
                    args = [dict(zip(columns, i)) for i in df.values]
                    self.dst_obj.merge(self.dst_tab, args, self.unique, num=self._insert_count)
                    self.task.update(self.last_time)
                else:
                    args = list(map(tuple, df.values))
                    self.dst_obj.insert(sql, args, num=self._insert_count)
                    self.task.append(self.last_time)
            else:
                args = list(map(tuple, df.values))
                if self.unique:
                    args = [dict(zip(columns, i)) for i in df.values]
                    self.dst_obj.merge(self.dst_tab, args, self.unique, num=self._insert_count)
                    self.task.append()
                else:
                    self.dst_obj.insert(sql, args, num=self._insert_count)
                    self.task.append()
            self.dst_obj.commit()
        log.info('插入数量：%s' % len(df))
