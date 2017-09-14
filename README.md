py_etl frame based on pandas and sqlalchemy

'1.1.0'
1.增加gis模块

'1.0.4'
1.修改应pandas字段名称改变导致的异常，pandas默认将类似T1.A rename为A
2.print 打印模块名称只显示mylogger的问题

'1.0.3'
1.第一次全量更新不使用merge, 直接insert ,提高第一次全量效率
2.update_field参数现在可以不传，用于每次全量同步数据场景
3.增加打印log模块

'1.0.0'
1.增加db数据库操作模块