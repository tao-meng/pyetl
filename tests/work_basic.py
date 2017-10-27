from context import Etl
from config import config


def f1(x):
    return x + "2017"


def task1():
    src_tab = 'py_etl'
    src_update = 'dtime'
    dst_tab = 'PY_ETL'
    dst_unique = 'FOO'
    mapping = {'FOO': 'id', 'fssj': 'dtime', 'BAR': 'foo'}
    app = Etl(src_tab, dst_tab, mapping, update=src_update, unique=dst_unique)
    app.config(config['testing1'])
    app.save(app.run(days=0))
    # app.save(app.run())/


def task2():
    src_tab = 'ACCIDENT'
    src_update = 'fssj'
    dst_tab = 'PY_ETL'
    dst_unique = 'FOO'
    mapping = {'FOO': 'SGDH', 'fssj': 'fssj', 'BAR': 'SGDDMS'}
    app = Etl(src_tab, dst_tab, mapping, update=src_update, unique=dst_unique)
    app.config(config['testing2'])
    app.add('FOO')(f1)
    app.save(app.run(days=0))


if __name__ == '__main__':
    task1()
    task2()
