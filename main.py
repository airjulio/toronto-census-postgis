import json


def read_geojson(path):
    with open(path) as f:
        return json.load(f)


def get_polygons(json_data):
    for f in json_data['features']:
        census_id = f['properties']['census_id']
        hood_id = f['properties']['hood_id']
        neighbourhood = f['properties']['neighbourhood']
        coordinates = f['geometry']['coordinates'][0]
        yield census_id, hood_id, neighbourhood, coordinates


if __name__ == '__main__':
    data = read_geojson('features.json')

    with open('census_tracts.sql', 'w') as fout:
        for cid, hid, neigh, coors in get_polygons(data):
            coors_str = ", ".join([f"{x} {y}" for x, y in coors])
            neigh = neigh.replace("'", "''")
            q = f"""insert into cha_geo.census_tracts (id, hood_id, neighbourhood, polygon) values('{cid}','{hid}', '{neigh}', ST_GeometryFromText('POLYGON(({coors_str}))'));\n"""
            fout.write(q)
