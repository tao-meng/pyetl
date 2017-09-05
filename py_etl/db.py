from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DatabaseError
from collections import OrderedDict
from sqlalchemy import engine
import re
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class Connection():

    def __init__(self, con, echo=False):
        """
        >>>con
        "oracle+cx_oracle://jwdn:password@192.168.152.1:1521/xe"
        """
        self._eng = con if isinstance(
            con, engine.base.Engine) else create_engine(con, echo=echo)
        self.session = self.create_session()

    def create_session(self):
        DB_Session = sessionmaker(bind=self._eng)
        return DB_Session()

    def execute(self, sql, args=None):
        """
        执行sql
        """
        return self.session.execute(sql, args)

    def query(self, sql, args=None):
        """
        查询返回元祖数据集(clob对象转对应字符串)
        """
        rs = self.execute(sql).fetchall()
        return rs

    def query_dict(self, sql, args=None):
        """
        查询返回字典数据集
        """
        rs = self.execute(sql)
        colunms = [i[0] for i in rs._cursor_description()]
        return [OrderedDict(zip(colunms, i)) for i in rs.fetchall()]

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
            count = 0
            if args and not isinstance(args, dict)\
                    and isinstance(args[0], dict):
                for i in range(0, len(args), num):
                    rs = self.execute(sql, args[i:i + num])
                    count += rs.rowcount
            else:
                rs = self.execute(sql, args)
                count = rs.rowcount
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
                sql_query = "select data_type from user_tab_columns\
                    where table_name='%s' and column_name='%s'" % (
                    table_name, column_name)
                data_type = self.query(sql_query)[0][0]
                sql_modify = ("alter table {table_name} modify({column_name}"
                              " {data_type}({value}))".format(
                                  table_name=table_name,
                                  column_name=column_name,
                                  data_type=data_type,
                                  value=int(value) + delta))
                self.session.execute(sql_modify)
                count += self.insert(sql, args, num)
            else:
                if args and not isinstance(args, dict)\
                        and isinstance(args[0], (tuple, list, dict)):
                    print('\nSQL EXECUTEMANY ERROR\n', sql)
                    for i in args[i:i + num][:20]:
                        print(i)
                    print('SQL EXECUTEMANY ERROR\n')
                # if args and isinstance(args[0], (tuple, list)):
                #     print('\nSQL EXECUTEMANY ERROR\n',
                #           sql, args[:20],
                #           '\nSQL EXECUTEMANY ERROR\n')
                else:
                    print('\nSQL EXECUTE ERROR\n',
                          sql, args,
                          '\nSQL EXECUTE ERROR\n')
                raise reason
        return count

    def merge(self, table, args, columns, unique):
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
        print(self.insert(sql, args))

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
        self.session.rollback()

    def commit(self):
        """
        提交
        """
        self.session.commit()

    def close(self):
        """
        关闭数据库连接
        """
        self.session.close()

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


if __name__ == "__main__":
    db_uri = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    with Connection(db_uri) as db:
        sql = "select * from test where rownum<2"
        print(db.query_dict(sql))
        # sql = "insert into test(id,foo) values(:id,:foo)"
        # print(db.insert(sql, {'id': 1111, 'foo': '111111'}))
        # print(db.delete_repeat('test', 'id', 'dtime'))
        # db.merge('test', {'foo': '1', 'bh': 2222}, ['foo', 'bh'], 'foo')
