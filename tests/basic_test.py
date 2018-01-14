from context import Etl
from config import config
from dateutil import parser
from env_config import env_config


def task_fromfile():
    src_tab = 'csv'
    dst_tab = 'py_etl_src'
    dst_unique = 'id'
    mapping = {'id': 'id', 'foo': 'foo', 'date_time': 'date_time',
               'x': 'x', 'y': 'y'}
    app = Etl(src_tab, dst_tab, mapping, unique=dst_unique)
    app.config(config['fromfile'])
    app.add('date_time')(lambda x: parser.parse(x) if x else None)
    app.save(app.run(days=0, where="rownum<10"))


def task_oracle():
    src_tab = 'py_etl_src'
    src_update = 'date_time'
    dst_tab = 'py_etl_dst'
    dst_unique = 'id'
    mapping = {'id': 'id', 'bar': 'foo', 'update_time': 'date_time',
               'lon': 'x', 'lat': 'y'}
    app = Etl(src_tab, dst_tab, mapping, update=src_update, unique=dst_unique)
    app.config(config['oracle'])
    app.save(app.run(days=0, where="rownum<10"))


def task_tofile():
    src_tab = 'py_etl_src'
    dst_tab = 'csv'
    dst_unique = 'id'
    mapping = {'id': 'id', 'bar': 'foo', 'update_time': 'date_time',
               'lon': 'x', 'lat': 'y'}
    app = Etl(src_tab, dst_tab, mapping, unique=dst_unique)
    app.config(config['tofile'])
    app.save(app.run(days=0, where="rownum<10"))


def main():
    with env_config():
        task_fromfile()
        # task_oracle()
        task_tofile()


if __name__ == '__main__':
    main()
