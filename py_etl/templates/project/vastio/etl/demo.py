from py_etl import Etl
from vastio.config import default
from vastio.lib import get_args
from vastio.logger import module_log
log = module_log(__file__)
src_tab = "SRC_TAB"
src_update = "SRC_UPDATE"
dst_tab = "DST_TAB"
dst_unique = "DST_UNIQUE"
mapping = {
    "ID": "CODE"
}


def task():
    args = get_args()
    app = Etl(src_tab, dst_unique, mapping,
              updte=src_update,
              unique=dst_unique)
    app.config(default)
    app.save(app.run(days=args.days))
    return app.db


def main():
    db = task()
    db.commit()


if __name__=="__main__":
    main()
