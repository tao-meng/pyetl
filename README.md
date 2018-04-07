## python etl frame

    from pyetl import Etl
    class TestOracleConfig:
        DST_URI = SRC_URI = {"uri": "oracle://lyt:lyt@local:1521/xe"}

    app = Etl('src_table', 'dst_table')
    app.config(TestOracleConfig)
    app.run()

### utf and other functions

    app = Etl('src_table', 'dst_table', unique='id')
    app.config(TestOracleConfig)

    @app.add('id')
    def udf(x):
        return x.upper()
    @app.after
    def clearup(app):
        print(app.src.read("select count(*) from dst_table"))
        app.dst.empty('src_table')

    app.run(where="rownum<10") # 数据源过滤