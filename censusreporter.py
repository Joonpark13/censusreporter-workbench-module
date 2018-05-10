# import pandas as pd

def render(table, params):
    import urllib.request as urlreq
    import ssl
    import json


    TOPIC_KEYS = ['B01001', 'B03002', 'B19001', 'B17001', 'B08006', 'B11002',
        'B12001', 'B13016', 'B25002', 'B25003', 'B25024', 'B25026', 'B25075',
        'B07003', 'B15002', 'B16007', 'B05006', 'B21002']
    GEO_KEYS = ['04000US01', '04000US02', '04000US04', '04000US05', '04000US06']


    # Modified from https://github.com/censusreporter/census-pandas/blob/master/util.py
    def get_data(tables=None, geoids=None, release='latest'):
        API_URL="http://api.censusreporter.org/1.0/data/show/{release}?table_ids={table_ids}&geo_ids={geoids}"

        if geoids is None:
            geoids = ['040|01000US']
        elif isinstance(geoids,str):
            geoids = [geoids]
        if tables is None:
            tables = ['B01001']
        elif isinstance(tables,str):
            tables=[tables]

        url = API_URL.format(table_ids=','.join(tables).upper(),
                             geoids=','.join(geoids),
                             release=release)

        ssl_context = ssl.SSLContext()
        ssl_context.verify_mode = ssl.CERT_NONE

        with urlreq.urlopen(url.format(tables, geoids), context=ssl_context) as response:
            return json.loads(response.read().decode('utf-8'))


    # From https://github.com/censusreporter/census-pandas/blob/master/util.py
    def prep_for_pandas(json_data,include_moe=False):
        # Given a dict of dicts as they come from a Census Reporter API call, set it up to be amenable to pandas.DataFrame.from_dict
        result = {}
        for geoid, tables in json_data.items():
            flat = {}
            for table,values in tables.items():
                for kind, columns in values.items():
                    if kind == 'estimate':
                        flat.update(columns)
                    elif kind == 'error' and include_moe:
                        renamed = dict((k+"_moe",v) for k,v in columns.items())
                        flat.update(renamed)
            result[geoid] = flat
        return result


    # Modified from https://github.com/censusreporter/census-pandas/blob/master/util.py
    def get_dataframe(tables=None, geoids=None, release='latest',geo_names=False,col_names=False,include_moe=False):
        response = get_data(tables=tables,geoids=geoids,release=release)
        frame = pd.DataFrame.from_dict(prep_for_pandas(response['data'],include_moe),orient='index')
        frame = frame[sorted(frame.columns.values)] # data not returned in order
        if geo_names:
            geo = pd.DataFrame.from_dict(response['geography'],orient='index')
            frame.insert(0,'name',geo['name'])
        if col_names:
            d = {}
            for table_id in response['tables']:
                colname_prepends = []
                columns = response['tables'][table_id]['columns']
                for column_id in columns:
                    colname = columns[column_id]['name']
                    indent = columns[column_id]['indent']

                    if indent is not None:
                        if indent > len(colname_prepends) - 1:
                            colname_prepends += [colname]
                        else:
                            colname_prepends = colname_prepends[:indent] + [colname]
                        d[column_id] = " ".join(colname_prepends[1:]) # Indent level 0 is always "Total:"
                    else:
                        d[column_id] = colname

            frame = frame.rename(columns=d)
        return frame



    topic_num = int(params['topic'])
    topic = TOPIC_KEYS[topic_num]

    sumlevel_num = int(params['sumlevel'])
    if sumlevel_num == 0:
        geo = "040%7C01000US" # all states


    return get_dataframe(topic, geo, geo_names=True, col_names=True)


if __name__ == "__main__":
    dframe = render(None, {'topic': 0, 'sumlevel':0})
    print(dframe)
