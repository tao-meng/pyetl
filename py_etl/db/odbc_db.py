# -*- coding: utf-8 -*-
from collections import OrderedDict
from pyodbc import DatabaseError
import pyodbc
import os
import re
if __name__ == '__main__':
    import sys
    sys.path.insert(
        0,
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    )
    from mylogger import log
else:
    from py_etl.mylogger import log
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def str_sql(field, split_str=','):
    """
    >>> str_sql(['id','name'])
    'id,name'
    >>> str_sql(['id','name'],split_str=';')
    'id;name'
    """
    return split_str.join(field)


def args_sql(field):
    """
    >>> args_sql(['id','name'])
    ':1,:1'
    """
    return ','.join([':1' for i in range(len(field))])


def print(*args, notice=''):
    log.debug('%s\n%s' % (notice, ' '.join(['%s' % i for i in args])))


def reduce_num(n, l):
    num = min(n, l)
    if num > 10:
        return num // 10
    else:
        return 1


class Connection():

    def __init__(self, con, echo=False):
        """
        >>>con
        'DSN=mpp_dsn'
        """
        print(self.__class__.__module__)
        self.connect = pyodbc.connect(con)
        self.cursor = self.connect.cursor()

    def execute(self, sql, args=None):
        """
        执行sql
        """
        sql = sql.replace(':1', '?')
        log.debug(sql)
        log.debug(args)
        if args:
            return self.cursor.execute(sql, args)
        else:
            return self.cursor.execute(sql)

    def _query_generator(self, sql, args, chunksize):
        """
        查询返回元祖数据集(clob对象转对应字符串)
        self.query(
            'SELECT * FROM oview.view_zz_jjjw_vw_accident_veh_rel limit ?',
            10)
        """
        rs = self.execute(sql, args)
        res = rs.fetchmany(chunksize)
        while res:
            yield res
            res = rs.fetchmany(chunksize)

    def query(self, sql, args=None, chunksize=None):
        """
        查询返回元祖数据集(clob对象转对应字符串)
        self.query(
            'SELECT * FROM oview.view_zz_jjjw_vw_accident_veh_rel limit ?',
            10)
        """
        if chunksize is None:
            rs = self.execute(sql, args).fetchall()
            return rs
        else:
            return self._query_generator(sql, args, chunksize)

    def _query_like_pandas_dataframe(self, sql, args, chunksize):
        """
        查询返回字典数据集
        self.query(
            'SELECT * FROM oview.view_zz_jjjw_vw_accident_veh_rel limit ?',
            10)
        """
        rs = self.execute(sql, args)
        colunms = [i[0] for i in self.cursor.description]
        res = rs.fetchmany(chunksize)
        while res:
            # dataframe = []
            # for i, j in enumerate(colunms):
            #     dataframe.append((j, list(map(lambda x: x[i], res))))
            dataframe = list(zip(colunms, zip(*res)))
            print(dataframe)
            yield dataframe
            res = rs.fetchmany(chunksize)

    def query_dict(self, sql, args=None, chunksize=None):
        """
        查询返回字典数据集
        self.query(
            'SELECT * FROM oview.view_zz_jjjw_vw_accident_veh_rel limit ?',
            10)
        """
        if chunksize is None:
            rs = self.execute(sql, args)
            colunms = [i[0] for i in self.cursor.description]
            return [OrderedDict(zip(colunms, i)) for i in rs.fetchall()]
        else:
            return self._query_like_pandas_dataframe(sql, args, chunksize)

    def insert(self, sql, args, num=10000):
        """
        批量更新插入，默认超过10000条数据自动commit
        insertone:
            self.insert(
                "insert into test(id) values(:id)",
                {'id': 6666}
            )
        insertmany:
            self.insert(
                "insert into test(id) values(:id)",
                [{'id': 6666}, {'id': 8888}]
            )
        @num:
            批量插入时定义一次插入的数量，默认10000
        """
        try:
            length = len(args)
            if args and not isinstance(args, dict)\
                    and isinstance(args[0], (tuple, list, dict)):
                for i in range(0, length, num):
                    self.cursor.executemany(sql, args[i:i + num])
            else:
                rs = self.execute(sql, args)
        except DatabaseError as reason:
            self.rollback()
            # match_name = re.compile('(?:into|update) +(\S+)')
            # table_name = match_name.findall(sql)[0]
            # cx_Oracle.DatabaseError: ORA-12899:
            # 列 "PPS1DBA"."PATROL_WPXX"."OBJ_NAME" 的值太大 (实际值: 40, 最大值: 25)
            if '的值太大' in str(reason):
                match_reason = re.compile(
                    r'"\w+"\."(\w+)"\."(\w+)" 的值太大 \(实际值: (\d+),')
                rs = match_reason.search(str(reason))
                table_name, column_name, value = (
                    rs.group(1), rs.group(2), rs.group(3))
                delta = 2 if len(
                    str(value)) == 1 else 2 * 10**(len(str(value)) - 2)
                sql_query = ("select data_type,char_used from user_tab_columns"
                             " where table_name='%s' and column_name='%s'" % (
                                 table_name, column_name))
                data_type, char_used = self.query(sql_query)[0]
                if char_used == 'C':
                    char_type = 'char'
                elif char_used == 'B':
                    char_type = 'byte'
                else:
                    char_type = ''
                sql_modify = ("alter table {table_name} modify({column_name}"
                              " {data_type}({value} {char_type}))".format(
                                  table_name=table_name,
                                  column_name=column_name,
                                  data_type=data_type,
                                  char_type=char_type,
                                  value=int(value) + delta))
                self.execute(sql_modify)
                self.insert(sql, args, num)
            else:
                if args and not isinstance(args, dict)\
                        and isinstance(args[0], (tuple, list, dict)):
                    if num <= 10 or length <= 10:
                        err_msg = ['\nSQL EXECUTEMANY ERROR\n %s' % sql]
                        for i in args[i:i + num]:
                            err_msg.append('\n %s' % i)
                        err_msg.append('\nSQL EXECUTEMANY ERROR\n')
                        log.error(''.join(err_msg))
                        raise reason
                    else:
                        self.insert(
                            sql, args[i:i + num],
                            num=reduce_num(num, length))
                else:
                    log.error(
                        '\nSQL EXECUTE ERROR\n%s\n%s\nSQL EXECUTE ERROR\n' %
                        (sql, args)
                    )
                    raise reason

    def merge(self, table, args, columns, unique, num=10000):
        param_columns = ','.join([':{0} as {0}'.format(i) for i in columns])
        update_field = ','.join(
            ['t1.{0}=t2.{0}'.format(i) for i in columns if i != unique])
        t1_columns = ','.join(['t1.{0}'.format(i) for i in columns])
        t2_columns = ','.join(['t2.{0}'.format(i) for i in columns])
        sql = ("MERGE INTO {table} t1"
               " USING (SELECT {param_columns} FROM dual) t2"
               " ON (t1.{unique}= t2.{unique})"
               " WHEN MATCHED THEN"
               " UPDATE SET {update_field}"
               " WHEN NOT MATCHED THEN"
               " INSERT ({t1_columns})"
               " VALUES ({t2_columns})".format(table=table,
                                               param_columns=param_columns,
                                               unique=unique,
                                               update_field=update_field,
                                               t1_columns=t1_columns,
                                               t2_columns=t2_columns))
        print(sql)
        self.execute(sql, args)
        # length = len(args)
        # for i in range(0, length, num):
        #     self.execute(sql, args[i:i + num])

    def delete_repeat(self, table, unique, cp_field="rowid"):
        """
        数据去重
        默认通过rowid方式去重
        """
        sql = "delete from {table} where {cp_field} is null".format(
            table=table, cp_field=cp_field)
        null_count = self.execute(sql).rowcount
        print(
            '删除对比字段(%s)中为空的数据：%s' % (cp_field, null_count)
        ) if null_count else None
        sql = ("delete from {table} where"
               " ({id}) in (select {id} from {table} GROUP BY {id}"
               " HAVING count({one_of_id})>1) and ({id},{cp_field}) not in"
               " (select {id},max({cp_field}) from {table} GROUP BY {id}"
               " HAVING count({one_of_id})>1)".format(
                   table=table, id=unique, cp_field=cp_field,
                   one_of_id=unique.split(',')[0]))
        rs = self.execute(sql)
        count = rs.rowcount
        print('删除重复数据：%s' % count)
        return count

    def rollback(self):
        """
        数据回滚
        """
        self.connect.rollback()

    def commit(self):
        """
        提交
        """
        self.connect.commit()

    def close(self):
        """
        关闭数据库连接
        """
        self.cursor.close()
        self.connect.close()

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalue, traceback):
        try:
            if exctype is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()


def ModTest():
    import logging, datetime
    log.setLevel(logging.DEBUG)
    # sql = "SELECT * FROM oview.view_zz_jjjw_vw_accident_veh_rel limit 2"
    # with Connection("DSN=mpp_dsn") as db:
    #     print(db.query(sql))
    sql = "SELECT * FROM TEST where dtime>?"
    with Connection('DSN=mydb;UID=root;PWD=password') as db:
        res = db.query(sql, [datetime.datetime(2017, 9, 20)])
        print((res))


if __name__ == '__main__':
    ModTest()
