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


if __name__ == "__main__":
    SRC_DB_URI = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    db = connection(SRC_DB_URI)
    rs = db.query('select * from test')
    print(rs)
    db.close()
    with connection(SRC_DB_URI) as db:
        rs = db.query('select * from test')
        print(rs)
