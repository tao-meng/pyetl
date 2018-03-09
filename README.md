## python etl frame

    from py_etl import Etl

    class TestOracleConfig:
        DST_URI = SRC_URI = {"uri": "oracle://jwdn:jwdn@local:1521/xe", "debug": True}

    app = Etl('src_table', 'dst_table', unique='id')
    app.config(TestOracleConfig)
    @app.add('id')
    def foo(x):
        return x.upper()
    @app.after
    def clear(app):
        app.src.empty('src_table')
    app.save(app.run())