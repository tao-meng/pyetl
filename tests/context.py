__all__ = ['EtlUtil', 'connection', 'transform', 'geometry']
import os
import sys
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
)
from py_etl import EtlUtil
from py_etl import connection
from py_etl.gis import transform
from py_etl.gis import geometry


def test_import():
    # SRC_DB_URI = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    SRC_DB_URI = 'DSN=mydb;UID=root;PWD=password'
    with connection(SRC_DB_URI) as db:
        sql = "insert into test(id,name) values(?,?)"
        db.insert(sql, [1235123, 'tom'])
        rs = db.query_dict('select * from test')
        print(rs)


if __name__ == "__main__":
    test_import()
