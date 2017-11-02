from context import Etl
from config import config
from gps_translate import gcj02towgs84


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
    # app.save(app.run())


def f2(x, y):
    return gcj02towgs84(float(y), float(x)) if x and y else [x, y]


def task2():
    src_tab = 'ACCIDENT'
    src_update = 'fssj'
    dst_tab = 'PY_ETL'
    dst_unique = 'FOO'
    mapping = {'FOO': 'SGDH', 'fssj': 'fssj', 'BAR': 'SGDDMS',
               'X': 'GPS_LAT', 'Y': 'GPS_LON'}
    app = Etl(src_tab, dst_tab, mapping, update=src_update, unique=dst_unique)
    app.config(config['testing2'])
    app.add('FOO')(f1)
    app.add(['x', 'y'])(f2)
    app.save(app.run(days=0, where="rownum<10"))


if __name__ == '__main__':
    # task1()
    task2()
