
class TestingConfig:
    DEBUG = True
    SRC_DB_URI = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    DST_DB_URI = SRC_DB_URI
    # TASK_TABLE = 'py_script_task'
    # QUERY_SIZE = 2000000
    # INSERT_SIZE = 200000
    # NEW_TABLE_FIELD_DEFAULT_SIZE = 200


class DevelopmentConfig:
    SRC_DB_URI = "oracle+cx_oracle://zzjwdn:zzjwdn@172.16.11.120:1521/dcenter"
    DST_DB_URI = "oracle+cx_oracle://jwdn:jwdn_admin@172.16.11.144:1521/jwdn"


config = {
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


if __name__ == "__main__":
    import os
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    eng = create_engine(
        "oracle+cx_oracle://jwdn:password@local:1521/xe", echo=True
    )
    # rs = eng.execute("insert into test(id) values(4444)")
    # print(rs.fetchall())
    DB_Session = sessionmaker(bind=eng)
    session = DB_Session()
    session.execute("insert into test(id) values(:id)", [{'id': 5555}])
    session.commit()
