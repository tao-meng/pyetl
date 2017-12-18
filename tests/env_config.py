from py_db import connection
from contextlib import contextmanager
from config import config


def concat_place(field, place=":1"):
    """
    >>> concat_place(['id','name'])
    ':1,:1'
    """
    return ','.join([place for i in range(len(field))])


@contextmanager
def env_config():
    db = connection(**config['default'].SRC_URI)
    db.insert("""
create table py_etl_src(
    id varchar(20) primary key,
    foo varchar(100),
    date_time date,
    x number(9,6),
    y number(9,6))""")
    db.insert("""
create table py_etl_dst(
    id varchar(20),
    bar varchar(100),
    update_time date,
    lon number(9,6),
    lat number(9,6))
""")
    try:
        yield
    finally:
        db.insert("drop table py_etl_src")
        db.insert("drop table py_etl_dst")


if __name__=="__main__":
    with env_config():
        print('test')
