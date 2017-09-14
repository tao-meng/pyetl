from context import Connection
from context import geometry
import itertools


def distance_to_point(m, n):
    '''
    点到点的距离
    '''
    m = [float(i) for i in m]
    n = [float(i) for i in n]
    a = m[0] - n[0]
    b = m[1] - n[1]
    dd = (a**2 + b**2) ** 0.5
    return dd


def f(l):
    # print(l)
    return ','.join([' '.join(i) for i in l if i])


def f1(p):
    return [tuple(i.split(' ')) for i in p.split(',')]


def f2(a, b):
    if a:
        for i in range(len(a) - 1, -1, -1):
            if distance_to_point(a[-1], a[i - 1]) > 0.001:
                break
    else:
        i = None
    if b:
        for j in range(len(b)):
            if distance_to_point(b[0], b[j]) > 0.001:
                break
    else:
        j = None
    return i, j


def f3(IN_LINK_ID, OUT_LINK_ID):
    IN = roads.get(record[IN_LINK_ID], None)
    OUT = roads.get(record[OUT_LINK_ID], None)
    i, j = f2(IN, OUT)
    if IN and OUT:
        print(i, j)
        print(roads[record[IN_LINK_ID]])
        print(roads[record[OUT_LINK_ID]])
        rs = [roads[record[IN_LINK_ID]][i:], roads[record[OUT_LINK_ID]][:j]]
        roads[record[IN_LINK_ID]] = roads[record[IN_LINK_ID]][:i + 1]
        update(record[IN_LINK_ID])
        roads[record[OUT_LINK_ID]] = roads[record[OUT_LINK_ID]][j - 1:]
        update(record[OUT_LINK_ID])
        print(rs)
        return rs
    elif IN:
        cross_road.extend(roads[record[IN_LINK_ID]][i:])
        roads[record[IN_LINK_ID]] = roads[record[IN_LINK_ID]][:i + 1]
        update(record[IN_LINK_ID])
        return []
    elif OUT:
        cross_road.extend(roads[record[OUT_LINK_ID]][:j])
        roads[record[OUT_LINK_ID]] = roads[record[OUT_LINK_ID]][j - 1:]
        update(record[OUT_LINK_ID])
        return []


def update(id):
    points = roads[id]
    sql = ("update VST_ENTITY_ROAD_SECTION "
           "set polygon=:polygon where section_id=:section_id")
    args = {}
    args['polygon'] = ','.join([' '.join(i) for i in points])
    args['section_id'] = id
    print(args)
    db.insert(sql, args)


def gen_area(x, y):
    offset = 0.0006
    area = [
        (x + offset, y + offset), (x + offset, y - offset),
        (x - offset, y - offset), (x - offset, y + offset),
    ]
    return area


def ff(loc, line):
    offset = 0.0006
    x = [loc[0] + offset, loc[0] - offset]
    y = [loc[1] + offset, loc[1] - offset]
    for i in (0, 1):
        if line[0][0] <= x[i] <= line[1][0]:
            return geometry.result(*line, x=x[i])
        if line[0][1] <= y[i] <= line[1][1]:
            return geometry.result(*line, x=y[i])


if __name__ == "__main__":
    loc = (31.327578, 120.715919)
    polygon = gen_area(*loc)
    db_uri = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    with Connection(db_uri) as db:
        rs = db.query(
            "select section_id,polygon from VST_ENTITY_ROAD_SECTION")
        roads = dict([(i, f1(j)) for i, j in rs])
        # rs = itertools.chain(*roads.values())
    for r in roads:
        for i, p in enumerate(roads[r]):
            idx1 = None
            idx2 = None
            if geometry.is_point_in_polygon(p, polygon):
                idx1 = i
            else:
                idx2 = i
            if idx1 is not None and idx2 is not None:
                ff(loc, (roads[r][idx1], roads[r][idx2]))
