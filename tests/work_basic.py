from context import Etl
from config import config


def job1():
    Etl.config(config['testing1'])
    src_tab = 'test'
    dst_tab = 'work'
    mapping = {'ID': 'id',
               'fssj': 'dtime',
               'foo': 'foo'}
    update = 'dtime'
    unique = 'ID'
    job = Etl(src_tab, dst_tab, mapping, update, unique)
    job.join(job.run())


def job2():
    Etl.config(config['testing2'])
    src_tab = 'ACCIDENT'
    dst_tab = 'PY_ETL'
    update = 'fssj'
    unique = 'FOO'
    mapping = {'FOO': 'SGDH',
               'fssj': 'fssj',
               'BAR': 'SGDDMS'}
    app = Etl(src_tab, dst_tab, mapping, update=update, unique=unique)
    app.add('BAR')(f1)
    app.save(app.run(where="rownum<100"))


def f1(x):
    return x + "2017"


if __name__ == '__main__':
    # job1()
    job2()
