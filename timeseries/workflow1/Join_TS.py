# process to (inner) join two time series.
# in: json file1, json file2
# out: json file3, json file4

# author: anuragsrivastav@rti.org
# date: April 03, 2019
# version: 1.0
# revision notes:
#
#
#

import json
import pandas as pd
from pandas.io.json import json_normalize


class JoinTS(object):

    def __init__(self, in_file1, in_file2, out_file1, out_file2):
        self.in_file1 = in_file1
        self.in_file2 = in_file2
        self.out_file1 = out_file1
        self.out_file2 = out_file2

    def process(self):
        [meta1, df1] = self.read_json(self.in_file1)
        [meta2, df2] = self.read_json(self.in_file2)

        df = pd.merge(df1, df2, on='date', how='inner')
        print(df.head())

        df1 = pd.DataFrame({'date': df['date'], 'value': df['value_x']})
        df1['date'] = df1['date'].dt.strftime('%Y-%m-%d')

        df2 = pd.DataFrame({'date': df['date'], 'value': df['value_y']})
        df2['date'] = df2['date'].dt.strftime('%Y-%m-%d')

        self.write_json(meta1, df1, self.out_file1)
        self.write_json(meta2, df2, self.out_file2)

        print('join {0},{1}...completed.'.format(self.in_file1, self.in_file2))
        return None

    @staticmethod
    def read_json(in_file):
        df = None
        meta_info = None
        try:
            with open(in_file) as json_file:
                data = json.load(json_file)
                meta_info = data['metaInfo']
                df = json_normalize(data['data'])

                # convert the column to DateTime format
                df['date'] = pd.to_datetime(df.date)
                df['index'] = df['date']
                df = df.set_index('index')
        except Exception as e:
            print(str(e))
        return [meta_info, df]

    @staticmethod
    def write_json(meta_data, df, out_file):
        data = {'metaInfo': meta_data, 'data': df.to_dict('records')}
        with open(out_file, 'w') as f:
            json.dump(data, f, indent=4)
        return None


# n = r'C:\Users\anuragsrivastav\Desktop\nwm\neponset_river\5866747_5866787.json'
# u = r'C:\Users\anuragsrivastav\Desktop\nwm\neponset_river\01105000_filled.json'
# n1 = r'C:\Users\anuragsrivastav\Desktop\nwm\neponset_river\5866747_5866787_joined.json'
# u1 = r'C:\Users\anuragsrivastav\Desktop\nwm\neponset_river\01105000_joined.json'
# j = JoinTS(n,u, n1, u1)
# j.process()
