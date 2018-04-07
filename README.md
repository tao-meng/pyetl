## python etl frame

    from py_etl import Etl

    class TestOracleConfig:
        DST_URI = SRC_URI = {"uri": "oracle://jwdn:jwdn@local:1521/xe", "debug": True}

    app = Etl('src_table', 'dst_table', unique='id')
    app.config(TestOracleConfig)
    @app.add('id')
    def udf(x):
        return x.upper()
    @app.after
    def clearup(app):
        print(app.dst.query("select count(*) from dst_table"))
        app.src.empty('src_table')
    app.run(where="rownum<10")