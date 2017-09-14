"""
地图距离相关计算
"""
import math


def translate(p, m, n, offset=0.001):
    '''
    坐标系旋转公式
    x' = x*cosθ + y*sinθ
    y' = y*cosθ - x*sinθ
    '''
    p = (p[0], p[1])
    m = (m[0], m[1])
    n = (n[0], n[1])
    angle = math.atan((m[1] - n[1]) /
                      (m[0] - n[0]))
    sin_a = math.sin(angle)
    cos_a = math.cos(angle)
    x1 = m[0] * cos_a + m[1] * sin_a
    x2 = n[0] * cos_a + n[1] * sin_a
    y1 = m[1] * cos_a - m[0] * sin_a
    # y2 = n[1] * cos_a - n[0] * sin_a
    px = p[0] * cos_a + p[1] * sin_a
    py = p[1] * cos_a - p[0] * sin_a
    if min(x1, x2) <= px <= max(x1, x2) and y1 - offset < py < y1 + offset:
        dd = abs(py - y1)
        return dd
    return False


def line_param(m, n):
    """
    计算直线表达式ax+by+c=0的a,b,c值
    >>> line_param((0, 1), (1, 0))
    (1, 1, -1)
    """
    a = m[1] - n[1]
    b = n[0] - m[0]
    c = -(a * m[0] + b * m[1])
    return a, b, c


def cross_point(line1, line2):
    '''
    直线相交点计算
    >>> line1 = [(0, 1), (1, 0)]
    >>> line2 = [(0, -1), (1, 0)]
    >>> cross_point(line1, line2)
    (1.0, 0.0)
    '''
    a1, b1, c1 = line_param(*line1)
    a2, b2, c2 = line_param(*line2)
    return ((b1 * c2 - b2 * c1) / (a1 * b2 - a2 * b1),
            (a2 * c1 - a1 * c2) / (a1 * b2 - a2 * b1))


def distance_to_point(m, n):
    '''
    点到点的距离
    '''
    a = m[0] - n[0]
    b = m[1] - n[1]
    dd = (a**2 + b**2) ** 0.5
    return dd


def distance_to_line(p, m, n):
    '''
    点到直线的距离
    直线公式: Ax+By+C=0
    p = (Xo, Yo)
    dd = │AXo+BYo+C│/√(A²+B²)
    '''
    p = (p[0], p[1])
    m = (m[0], m[1])
    n = (n[0], n[1])
    a = m[1] - n[1]
    b = n[0] - m[0]
    c = -a * m[0] - b * m[1]
    dd = abs(a * p[0] + b * p[1] + c) / ((a**2 + b**2)**0.5)
    return dd


def line_near_point(p, m, n, offset=0.001):
    '''
    offset 是坐标的偏移距离，转换到实际距离0.001约等于100m
    粗虐计算，没有point_near_line方法精确，可以提升运算速度
    加法运算对速度影响很小，创建对象对速度有影响, float和调用函数的操作开销是比较大的
    '''
    if min(m[0], n[0]) < p[0] < max(m[0], n[0]) + offset and \
            min(m[1], n[1]) - offset < p[1] < max(m[1], n[1]) + offset:
        return True
    return False


def distance_to_line_segment(p, m, n):
    '''
    用向量判断点到线段的距离
    '''
    x, y = p
    x1, y1 = m
    x2, y2 = n
    cross = (x2 - x1) * (x - x1) + (y2 - y1) * (y - y1)
    d2 = (x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1)
    # r = cross / d2
    # px = x1 + (x2 - x1) * r
    # py = y1 + (y2 - y1) * r
    if cross <= 0:
        return ((x - x1) * (x - x1) + (y - y1) * (y - y1))**0.5
    elif cross >= d2:
        return ((x - x2) * (x - x2) + (y - y2) * (y - y2))**0.5
    else:
        # return ((x - px) * (x - px) + (py - y1) * (py - y1))**0.5
        return distance_to_line(p, m, n)


def is_point_in_polygon(point, poly):
    """
    判断点是否在图形内部
    通过向一个方向分割坐标系,通过判断点在被分割的区域数量是奇数还是偶数来确定,奇数在图形内部,偶数图形外
    """
    num = len(poly)
    ii = 0
    jj = num - 1
    result = False
    for ii in range(num):
        if ((poly[ii][1] > point[1]) != (poly[jj][1] > point[1])) and (
            point[0] < (
                (poly[jj][0] - poly[ii][0]) * (point[1] - poly[ii][1]) /
                (poly[jj][1] - poly[ii][1]) + poly[ii][0]
            )
        ):
            result = not result
        jj = ii
    return result


def roads_near_point(location, all_road):
    """
    简单的确定下点
    """
    # 定义偏移量大小
    offset = 0.001
    near_roads = {}
    for id, points in all_road.items():
        p, m, n = location, points[0], points[-1]
        if min(m[0], n[0]) - offset < p[0] < max(m[0], n[0]) + offset and \
                min(m[1], n[1]) - offset < p[1] < max(m[1], n[1]) + offset:
            near_roads[id] = points
    return near_roads


def point_link_road(location, near_roads):
    """
    点周围最近的路
    """
    roads = {}
    for i, j in near_roads.items():
        points = list(set(j))
        points.sort(key=j.index)
        # 约100km
        dd_min = 1
        for ii in range(len(points) - 1):
            dd = distance_to_line_segment(
                location, points[ii], points[ii + 1])
            dd_min = min(dd, dd_min)
        roads[i] = dd_min
    road_id = min(roads.items(), key=lambda x: x[1])[0]
    return road_id


def point_link_roads(location, near_roads):
    """
    点周围50m范围的路
    """
    roads = {}
    for i, j in near_roads.items():
        points = list(set(j))
        points.sort(key=j.index)
        # 约100km
        dd_min = 1
        for ii in range(len(points) - 1):
            dd = distance_to_line_segment(
                location, points[ii], points[ii + 1])
            dd_min = min(dd, dd_min)
        roads[i] = dd_min

    tmp = list(filter(lambda x: x[1] < 0.0005, roads.items()))
    if tmp:
        tmp.sort(key=lambda x: x[1])
        road_ids = [i[0] for i in tmp]
        return road_ids
    else:
        return None


def min_index_list(d, d_min):
    '''
    列表中最小值的所有索引以列表返回
    '''
    index_list = []
    min_index = d.index(d_min)
    index_list.append(min_index)
    if min_index == len(d) - 1:
        rs = None
    else:
        rs = min_index_list(
            d[min_index + 1:], d_min
        ) if min(d[min_index + 1:]) == d_min else None
    if rs:
        index_list.extend([i + min_index + 1 for i in rs])
    return index_list


def str_to_points(string):
    points = []
    for pp in string.split(','):
        v = pp.split()
        points.append((float(v[0]), float(v[1])))
    return points


def acdnt_belong_to_road(location, all_road):
    """
    事故所属路段
    """
    # 筛选事故点周边路段
    near_roads = roads_near_point(location, all_road)
    # test(near_roads)
    # 精确定位事故路段
    result = point_link_road(location, near_roads) if near_roads else None
    return result


def police_to_road(location, all_road):
    """
    警员所属路段
    """
    # 筛选警员周边路段
    near_roads = roads_near_point(location, all_road)
    # 精确定位警员路段
    result = point_link_roads(location, near_roads) if near_roads else None
    return result


def points_belong_to(points, rs):
    """
    点集属于哪个区域
    points 对应一条路的坐标
    """
    point = [float(p) for p in points[0].split()]
    for i, j in rs:
        polygon = [[float(x) for x in p.split()] for p in j.split(',')]
        result = is_point_in_polygon(point, polygon)
        if result:
            return i
    else:
        return points_belong_to(points[1:], rs) if len(points) != 1 else None


def point_belong_to(point, rs):
    """
    点属于哪个区域
    """
    for i, j in rs:
        polygon = [[float(x) for x in p.split()] for p in j.split(',')]
        result = is_point_in_polygon(point, polygon)
        if result:
            return i


def road_belong_to(unique_id, colume_polygon, colume_belong, table_name, name_type='code'):
    """
    道路所属区域
    """
    with connection(oracle_uri) as db:
        rs = db.query(
            'select %s,polygon from VST_ENTITY_PATROL_AREA' % name_type)
        sql = 'select %s,%s from %s' % (unique_id, colume_polygon, table_name)
        road_points = db.query(sql, clob=1)
        # print(road_points[0])
        dict_section_id_to_area = {}
        for section_id, points in road_points:
            points = points.split(',')
            area = points_belong_to(points, rs)
            dict_section_id_to_area[section_id] = area
        args = [(j, i) for i, j in dict_section_id_to_area.items()]
        sql = "update %s set %s=:1 where %s=:2" % (
            table_name, colume_belong, unique_id)
        db.insert_or_update(sql, args)


def location_belong_to(unique_id, colume_lon, colume_lat, colume_belong, table_name, name_type='code'):
    """
    坐标位置所属区域
    """
    with connection(oracle_uri) as db:
        rs = db.query(
            'select %s,polygon from VST_ENTITY_PATROL_AREA' % name_type)
        sql = 'select %s,%s,%s from %s' % (
            unique_id, colume_lon, colume_lat, table_name)
        acdnts = db.query(sql)
        dict_section_id_to_area = {}
        for id, x, y in acdnts:
            if x is None:
                continue
            point = (float(x), float(y))
            area = point_belong_to(point, rs)
            dict_section_id_to_area[id] = area
        args = [(j, i) for i, j in dict_section_id_to_area.items()]
        sql = "update %s set %s=:1 where %s=:2" % (
            table_name, colume_belong, unique_id)
        db.insert_or_update(sql, args)


def main():
    """
    本地数据库测试事故点所属路段
    """
    with connection(oracle_uri) as db:
        rs_road = db.query("select section_id,polygon from %s " %
                           'VST_ENTITY_ROAD_SECTION')
        all_road = {}
        for i, j in rs_road:
            all_road[i] = str_to_points(j)
        rs_points = db.query(
            "select ID,x,y from SG2 where x is not null")
        args = []
        for i, x, y in rs_points:
            location = (float(x), float(y))
            near_roads = roads_near_point(location, all_road)  # 筛选事故点周边路段
            # test(near_roads)
            if near_roads:
                result = point_link_road(location, near_roads)  # 精确定位事故路段
                args.append([i, result]) if result is not None else print(
                    i, y, ',', x)
        db.creat_table('sgld', ['acdnt_id', 'link_id'])
        sql = 'insert into sgld values(:1,:2)'
        db.insert_or_update(sql, args)


if __name__ == '__main__':
    # 测试性能
    # import cProfile
    # cProfile.run('main()')
    # road_belong_to(unique_id='id', colume_polygon='POLYGON', colume_belong='BELONG_TO',
    #                table_name='VST_ROAD_SECTION', name_type='patrol_id')
    # location_belong_to(unique_id='ID', colume_lon='X', colume_lat='Y', colume_belong='BELONG', table_name='SG2', name_type='code')
    # print(cross_point([(1, 1), (-1, 1)], [(0, -1), (0, 1)]))
    print(line_param((0,-1), (-1,0)))
