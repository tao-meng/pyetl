# -*- coding: utf-8 -*-
"""
    db connection
"""
__all__ = ['connection']

from . import odbc_db
from . import sqlalchemy_db
from sqlalchemy import engine


def connection(uri='DSN=mpp_dsn'):
    if isinstance(uri, engine.base.Engine) or '://' in uri:
        return sqlalchemy_db.Connection(uri)
    elif uri.startswith('DSN'):
        return odbc_db.Connection(uri)
    else:
        raise ValueError('输入的格式不正确')
