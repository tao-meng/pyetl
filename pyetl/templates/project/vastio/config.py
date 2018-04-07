# -*- coding: utf-8 -*-


class TestConfig:
    DEBUG = True
    SRC_URI = "oracle://jwdn:password@local:1521/xe"
    DST_URI = SRC_URI
    # SRC_PLACEHOLDER = "?"
    # TASK_TABLE = 'py_script_task'
    # QUERY_SIZE = 2000000
    # INSERT_SIZE = 200000
    # NEW_TABLE_FIELD_DEFAULT_SIZE = 200


class DevConfig:
    SRC_URI = 'oracle://sjksh:sjksh_szga@172.16.11.120:1521/dcenter'
    DST_URI = 'oracle://PPS1DBA:Peptalk123@172.16.11.58:1521/p570b'


class ComConfig:
    elastic = {'host': 'http://172.16.60.149:9200/'}
    database = "oracle://jwdn:jwdn@172.16.60.174:1521/ORCL"


cfg = {
    'test': TestConfig,
    "product": DevConfig,
    'default': DevConfig
}

config = cfg['default']
