from py_etl import Etl
from config import default


def task():
    src_table = 'SRC_TABLE T1'
    src_update = 'T1.UPDATE_TIME'
    dst_table = 'DST_TABLE2'
    dst_unique = 'ID'
    mapping = {'ID': 't1.BH'}
    app = Etl(src_table, dst_table, mapping, update=src_update, unique=dst_unique)
    app.config(default)
    app.save(app.run())


if __name__ == '__main__':
    task()
