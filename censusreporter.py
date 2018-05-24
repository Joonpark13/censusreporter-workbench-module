# import pandas as pd

def render(table, params):
    import urllib.request as urlreq
    import ssl
    import json


    TOPIC_KEYS = ['B01001', 'B01001', 'B03002', 'B19001', 'B17001', 'B08006',
        'B11002', 'B12001']
    STATE_FIPS = ["01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
        "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
        "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
        "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47",
        "48", "49", "50", "51", "53", "54", "55", "56", "60", "66", "69",
        "72", "78"]


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

                    # Prepend nested column names
                    if indent is not None:
                        if indent > len(colname_prepends) - 1:
                            colname_prepends += [colname]
                        else:
                            colname_prepends = colname_prepends[:indent] + [colname]

                        if indent == 0:
                            d[column_id] = colname
                        else:
                            d[column_id] = " ".join(colname_prepends[1:]) # Never want to prepend "Total:"
                    else:
                        d[column_id] = colname

            frame = frame.rename(columns=d)

        # Add geoid column
        parsed_geoids = sorted(response['geography'].keys())[1:] # First one is parent
        frame.insert(1, 'geoid', parsed_geoids) 

        return frame


    def get_dataframe_simple(topic_num, topic, geo):
        response = get_data(tables=topic, geoids=geo, release='latest')
        data = pd.DataFrame.from_dict(prep_for_pandas(response['data'], False), orient='index')
        data = data[sorted(data.columns.values)] # data not returned in order

        geo = pd.DataFrame.from_dict(response['geography'], orient='index')
        curated_data = pd.DataFrame.from_dict(geo['name'].iloc[1:])

        # Add geoid column
        parsed_geoids = sorted(response['geography'].keys())[1:] # First one is parent
        curated_data.insert(1, 'geoid', parsed_geoids) 

        # Column curation
        if topic_num == 0: # Age
            under_18 = data['B01001003'] + data['B01001004'] + data['B01001005'] + \
                data['B01001006'] + data['B01001027'] + data['B01001028'] + \
                data['B01001029'] + data['B01001030']
            curated_data.insert(2, 'Under 18', under_18)

            eighteen_to_64 = data['B01001007'] + data['B01001008'] + \
                data['B01001009'] + data['B01001010'] + data['B01001011'] + \
                data['B01001012'] + data['B01001013'] + data['B01001014'] + \
                data['B01001015'] + data['B01001016'] + data['B01001017'] + \
                data['B01001018'] + data['B01001019'] + data['B01001031'] + \
                data['B01001032'] + data['B01001033'] + data['B01001034'] + \
                data['B01001035'] + data['B01001036'] + data['B01001037'] + \
                data['B01001038'] + data['B01001039'] + data['B01001040'] + \
                data['B01001041'] + data['B01001042'] + data['B01001043']
            curated_data.insert(3, '18 to 64', eighteen_to_64)

            over_65 = data['B01001020'] + data['B01001021'] + \
                data['B01001022'] + data['B01001023'] + data['B01001024'] + \
                data['B01001025'] + data['B01001044'] + data['B01001045'] + \
                data['B01001046'] + data['B01001047'] + data['B01001048'] + \
                data['B01001049']
            curated_data.insert(4, 'Over 65', over_65)
            
        elif topic_num == 1: # Sex
            curated_data.insert(2, 'Male', data['B01001002'])
            curated_data.insert(3, 'Female', data['B01001026'])

        elif topic_num == 2: # Race
            curated_data.insert(2, 'White', data['B03002003'])
            curated_data.insert(3, 'Black', data['B03002004'])
            curated_data.insert(4, 'Native', data['B03002005'])
            curated_data.insert(5, 'Asian', data['B03002006'])
            curated_data.insert(5, 'Islander', data['B03002007'])
            curated_data.insert(5, 'Other', data['B03002008'])
            curated_data.insert(5, 'Two or More', data['B03002009'])
            curated_data.insert(6, 'Hispanic', data['B03002012'])

        elif topic_num == 3: # Household Income
            under_50k = data['B19001002'] + data['B19001003'] + \
                data['B19001004'] + data['B19001005'] + data['B19001006'] + \
                data['B19001007'] + data['B19001008'] + data['B19001009'] + \
                data['B19001010']
            curated_data.insert(2, 'Under $50K', under_50k)

            fifty_to_100k = data['B19001011'] + data['B19001012'] + data['B19001013']
            curated_data.insert(3, '$50K to $100K', fifty_to_100k)

            hundred_to_200k = data['B19001014'] + data['B19001015'] + data['B19001016']
            curated_data.insert(4, '$100K to $200K', hundred_to_200k)

            curated_data.insert(5, 'Over $200K', data['B19001017'])

        elif topic_num == 4: # Poverty
            poverty = data['B17001004'] + data['B17001005'] + \
                data['B17001006'] + data['B17001007'] + data['B17001008'] + \
                data['B17001009'] + data['B17001018'] + data['B17001019'] + \
                data['B17001020'] + data['B17001021'] + data['B17001022'] + \
                data['B17001023']
            curated_data.insert(2, 'Poverty, Children (Under 18)', poverty)

            non_poverty = data['B17001033'] + data['B17001034'] + \
                data['B17001035'] + data['B17001036'] + data['B17001037'] + \
                data['B17001038'] + data['B17001047'] + data['B17001048'] + \
                data['B17001049'] + data['B17001050'] + data['B17001051'] + \
                data['B17001052']
            curated_data.insert(3, 'Non-poverty, Children (Under 18)', non_poverty)

            poverty = data['B17001015'] + data['B17001016'] + data['B17001029'] + data['B17001030']
            curated_data.insert(4, 'Poverty, Seniors (65 and Over)', poverty)

            non_poverty = data['B17001044'] + data['B17001045'] + data['B17001058'] + data['B17001059']
            curated_data.insert(5, 'Non-poverty, Seniors (65 and Over)', non_poverty)

        elif topic_num == 5: # Transportation to Work
            curated_data.insert(2, 'Drove Alone', data['B08006003'])
            curated_data.insert(3, 'Carpooled', data['B08006004'])
            curated_data.insert(4, 'Public Transit', data['B08006008'])
            curated_data.insert(5, 'Bicycle', data['B08006014'])
            curated_data.insert(6, 'Walked', data['B08006015'])
            curated_data.insert(7, 'Other', data['B08006016'])
            curated_data.insert(8, 'Worked at Home', data['B08006017'])

        elif topic_num == 6: # Population by Household Type
            curated_data.insert(2, 'Married Couples', data['B11002003'])
            curated_data.insert(3, 'Male Householder', data['B11002006'])
            curated_data.insert(4, 'Female Householder', data['B11002009'])
            curated_data.insert(5, 'Non-family', data['B11002012'])

        elif topic_num == 7: # Marital Status by Sex
            curated_data.insert(2, 'Never Married: Male', data['B12001003'])
            curated_data.insert(3, 'Never Married: Female', data['B12001012'])
            curated_data.insert(4, 'Married: Male', data['B12001004'])
            curated_data.insert(5, 'Married: Female', data['B12001013'])
            curated_data.insert(6, 'Divorced: Male', data['B12001010'])
            curated_data.insert(7, 'Divorced: Female', data['B12001019'])
            curated_data.insert(8, 'Windowed: Male', data['B12001009'])
            curated_data.insert(9, 'Windowed: Female', data['B12001018'])

        return curated_data



    topic_num = int(params['topic'])
    topic = TOPIC_KEYS[topic_num]

    sumlevel_num = int(params['sumlevel'])
    if sumlevel_num == 0:
        geo = "040%7C01000US" # all states
    elif sumlevel_num == 1: # Counties
        state_selected = int(params['states-for-counties'])
        selected_state_fips = STATE_FIPS[state_selected]
        geo = "050%7C04000US" + selected_state_fips
    elif sumlevel_num == 2: # Places
        state_selected = int(params['states-for-places'])
        selected_state_fips = STATE_FIPS[state_selected]
        geo = "160%7C04000US" + selected_state_fips
    elif sumlevel_num == 3: # Metro Areas
        state_selected = int(params['states-for-metro-areas'])
        selected_state_fips = STATE_FIPS[state_selected]
        geo = "310%7C04000US" + selected_state_fips


    # return get_dataframe(topic, geo, geo_names=True, col_names=True)
    return get_dataframe_simple(topic_num, topic, geo)


if __name__ == "__main__":
    dframe = render(None, {'topic': 7, 'sumlevel': 1, 'states-for-counties': 0})
    print(dframe)
