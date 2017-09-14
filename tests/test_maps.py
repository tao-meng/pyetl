import numpy as np
# import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import gmplot
from context import transform, Connection


def to_points(r):
    return [p.split(' ') for p in r.split(',')]


def gen_road():
    eng = create_engine('oracle+cx_oracle://jwdn:password@local:1521/xe')
    sql = ("select polygon from vst_entity_road_section"
           " where section_id like '100000%'")
    with Connection(eng) as db:
        raw = db.query(sql)
    p = (120.720977, 31.301294)
    yy, xx = transform.wgs84togcj02(*p)
    mymap = gmplot.GoogleMapPlotter(xx, yy, 17)
    for rs in raw:
        ll = np.array(to_points(rs[0]))
        ll = ll.astype(np.float64)
        ll = np.array(list(map(lambda p: transform.wgs84togcj02(*p), ll)))
        y, x = ll[:, 0], ll[:, 1]
        # mymap.plot(x, y, "red", edge_width=5)
        # mymap.scatter(x, y, c='red', s=5, marker=False, alpha=0.5)
        mymap.scatter([xx], [yy], c='green', s=10, marker=False, alpha=0.5)
    mymap.draw("map.html")


def gen_points():
    eng = create_engine('oracle+cx_oracle://jwdn:password@local:1521/xe')
    sql = "select lon,lat from map_cross where rownum <5"
    with Connection(eng) as db:
        ll = np.array(db.query(sql))
    ll = ll.astype(np.float64)
    print(ll)
    ll = np.array(list(map(lambda p: transform.wgs84togcj02(*p), ll)))
    y, x = ll[:, 0], ll[:, 1]
    mymap = gmplot.GoogleMapPlotter(31.2152384, 120.6733304, 16)
    mymap.scatter(x, y, c='green', s=10, marker=False, alpha=0.5)
    mymap.draw("map.html")


if __name__ == '__main__':
    gen_road()
