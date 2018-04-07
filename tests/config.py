class TestingConfig:
    DEBUG = True
    # SRC_URI = {"uri": "DSN=mydb;UID=root;PWD=password", 'driver': 'pyodbc'}
    SRC_URI = {"uri": "DSN=mysqldb", 'driver': 'pyodbc'}
    DST_URI = "oracle://jwdn:jwdn@local:1521/xe"
    SRC_PLACEHOLDER = "?"
    # DST_PLACEHOLDER = ":1"
    # TASK_TABLE = 'task'
    # QUERY_COUNT = 2000000
    # INSERT_COUNT = 200000
    # CREATE_TABLE_FIELD_SIZE = 200


class TestoracleConfig:
    DEBUG = True
    QUERY_COUNT = 200
    SRC_URI = {"uri": "oracle://jwdn:jwdn@local:1521/xe", "debug": True}
    DST_URI = {"uri": "oracle://jwdn:jwdn@local:1521/xe", "debug": False}


class TestFromfileConfig:
    DEBUG = True
    SRC_URI = {'file': 'pyetl_src.csv'}
    DST_URI = "oracle://jwdn:jwdn@local:1521/xe"


class TestTofileConfig:
    DEBUG = True
    SRC_URI = "oracle://jwdn:jwdn@local:1521/xe"
    DST_URI = {'file': 'pyetl.csv'}


class DevelopmentConfig:
    SRC_URI = "oracle+cx_oracle://zzjwdn:zzjwdn@172.16.11.120:1521/dcenter"
    DST_URI = "oracle+cx_oracle://jwdn:jwdn_admin@172.16.11.144:1521/jwdn"


config = {
    'testing': TestingConfig,
    'fromfile': TestFromfileConfig,
    'tofile': TestTofileConfig,
    'oracle': TestoracleConfig,
    'default': TestoracleConfig
}
