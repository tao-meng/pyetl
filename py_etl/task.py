from py_db import connection
import datetime


# class taskUtil(base.Connection):

#     def __init__(self):
#         super().__init__()

#     def query(self, src_table, dst_table):
#         sql = ("select last_time from {task_table} "
#                "where src_table='{src_table}'"
#                "and dst_table='{dst_table}'".format(
#                    src_table=src_table,
#                    dst_table=dst_table))
#         return self.insert(sql)


# if __name__ == "__main__":
#     db = taskUtil("task.db", driver="sqlite3")


class TaskConfig(object):

    def __init__(self, src_table, dst_table, task_table="task",):
        self.db = connection('task.db', driver='sqlite3')
        self.task_table = task_table
        self.src_table = src_table
        self.dst_table = dst_table
        self.init_db()

    def exist_table(self):
        sql = ("SELECT COUNT(*) FROM sqlite_master"
               " where type='table' and name='%s'") % self.task_table
        return self.db.query(sql)[0][0]

    def init_db(self):
        sql = ("create table if not exists"
               " %s(id varchar(500) primary key,"
               " src_table varchar(400),"
               " dst_table varchar(100),"
               " last_time DATETIME)") % self.task_table
        self.db.insert(sql)

    def append(self, last_time=None):
        id = self.src_table + "_" + self.dst_table
        self.db.insert(
            "insert into %s(id,last_time,src_table,dst_table) "
            "values(:1,:1,:1,:1)" % self.task_table,
            [id, last_time, self.src_table, self.dst_table]
        )
        self.db.commit()

    def update(self, last_time):
        self.db.insert(
            "update %s set last_time=:1 "
            "where src_table=:1 and dst_table=:1" % self.task_table,
            [last_time, self.src_table, self.dst_table]
        )
        self.db.commit()

    def query(self):
        res = self.db.query(
            "select 1, last_time from {task_table} "
            "where src_table='{src_table}'"
            "and dst_table='{dst_table}'".format(
                task_table=self.task_table,
                src_table=self.src_table,
                dst_table=self.dst_table))
        dt = datetime.datetime.strptime(res[1], '%Y%m%d')
        return res[0], dt


if __name__ == "__main__":
    job = TaskConfig("tab1", "tab2")
    # job.append(datetime.datetime(2017, 9, 12).__str__())
    job.modify(datetime.datetime(2017, 10, 19, 14, 34).__str__())
    rs = job.check()
    print(rs)