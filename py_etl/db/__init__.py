# -*- coding: utf-8 -*-
"""
    db connection
"""
__version__ = '1.1.2'

__all__ = ['connection']

from . import odbc_db
from . import sqlalchemy_db


def connection(uri):
    if uri.startswith('DSN'):
        return odbc_db.Connection(uri)
    elif '://' in uri:
        return sqlalchemy_db.Connection(uri)
    else:
        raise ValueError('输入的格式不正确')
