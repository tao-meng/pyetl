class Testing1Config:
    DEBUG = True
    # SRC_URI = {"uri": "DSN=mydb;UID=root;PWD=password", 'driver': 'pyodbc'}
    SRC_URI = {"uri": "DSN=mysqldb", 'driver': 'pyodbc'}
    DST_URI = "oracle://jwdn:password@local:1521/xe"
    SRC_PLACEHOLDER = "?"
    # DST_PLACEHOLDER = ":1"
    # TASK_TABLE = 'task'
    # QUERY_COUNT = 2000000
    # INSERT_COUNT = 200000
    # CREATE_TABLE_FIELD_SIZE = 200


class Testing2Config:
    DEBUG = True
    SRC_URI = "oracle://jwdn:password@local:1521/xe"
    DST_URI = "oracle://jwdn:password@local:1521/xe"
    # SRC_URI = {"uri": "jwdn/password@local:1521/xe", "driver": "cx_Oracle"}
    # DST_URI = {"uri": "jwdn/password@local:1521/xe", "driver": "cx_Oracle"}


class DevelopmentConfig:
    SRC_DB_URI = "oracle+cx_oracle://zzjwdn:zzjwdn@172.16.11.120:1521/dcenter"
    DST_DB_URI = "oracle+cx_oracle://jwdn:jwdn_admin@172.16.11.144:1521/jwdn"


config = {
    'testing1': Testing1Config,
    'testing2': Testing2Config,
    'default': DevelopmentConfig
}
