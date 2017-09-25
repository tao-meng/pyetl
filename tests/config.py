class Testing1Config:
    DEBUG = True
    SRC_DB_URI = "DSN=mydb;UID=root;PWD=password"
    DST_DB_URI = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    # TASK_TABLE = 'py_script_task'
    # QUERY_SIZE = 2000000
    # INSERT_SIZE = 200000
    # NEW_TABLE_FIELD_DEFAULT_SIZE = 200


class Testing2Config:
    DEBUG = True
    SRC_DB_URI = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    DST_DB_URI = "oracle+cx_oracle://jwdn:password@local:1521/xe"


class DevelopmentConfig:
    SRC_DB_URI = "oracle+cx_oracle://zzjwdn:zzjwdn@172.16.11.120:1521/dcenter"
    DST_DB_URI = "oracle+cx_oracle://jwdn:jwdn_admin@172.16.11.144:1521/jwdn"


config = {
    'testing1': Testing1Config,
    'testing2': Testing2Config,
    'default': DevelopmentConfig
}
