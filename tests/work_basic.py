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

    # @job.add('ZB')
    # def f1(x):
    #     x.remove(None) if None in x else None
    #     return ','.join(x)
    job.join(job.run())


def job2():
    Etl.config(config['testing2'])
    src_tab = 'ETL_TEST_SRC'
    dst_tab = 'ETL_TEST_WORK1'
    mapping = {'ID': 'id',
               'fssj': 'dtime',
               'foo': 'foo'}
    update = 'dtime'
    # unique = 'ID'
    job = Etl(src_tab, dst_tab, mapping, update)

    # @job.add('foo')
    # def f1(x):
    #     return x + 'x'

    job.join(job.run())


if __name__ == '__main__':
    # job1()
    job2()
