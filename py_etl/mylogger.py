import logging
console = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s %(module)s/%(name)s: %(message)s')
console.setFormatter(formatter)
sql_log = logging.getLogger('SQL')
sql_log.addHandler(console)
sql_log.setLevel(logging.INFO)
log = logging.getLogger('Print')
log.addHandler(console)
log.setLevel(logging.INFO)


def print(*args, notice=''):
    log.debug('%s\n%s' % (notice, ' '.join(['%s' % i for i in args])))


if __name__ == '__main__':
    log.setLevel(logging.DEBUG)
    print('hello world')
