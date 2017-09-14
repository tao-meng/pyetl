from context import EtlUtil
from config import config
EtlUtil.config(config['testing'])
src_table = 'test'
dst_table = 'work'
# 表的相关字段映射
field_map = {'ID': 'id',
             'fssj': 'dtime',
             'foo': 'foo'}
update_field = 'dtime'
unique_field = 'ID'
job = EtlUtil(src_table, dst_table, field_map, update_field, unique_field)


# @job.add('ZB')
# def f1(x):
#     x.remove(None) if None in x else None
#     return ','.join(x)


if __name__ == '__main__':
    job.join(job.run())
