# process to adjust a time-series
# in: json file, output file path, bias json file, statistics
# out: json file

# author: anuragsrivastav@rti.org
# date: April 22, 2019
# version: 1.0
# revision notes:
#
#
#

import json
import pandas as pd
from pandas.io.json import json_normalize


class AdjustTS(object):

    def __init__(self, in_file, out_file, error_file, stat):
        self.in_file = in_file
        self.error_file = error_file
        self.out_file = out_file
        self.stat = stat

    def process(self):
        error = self.read_error_json()
        [meta, df] = self.read_json(self.in_file)

        q25 = df['value'].quantile(0.25)
        q50 = df['value'].quantile(0.5)
        q75 = df['value'].quantile(0.75)
        qmax = df['value'].quantile(1.0)

        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['value'] = df.apply(lambda d: (d['value'] + error.loc[self.stat, 'v<=p0.25']) if d['value'] <= q25 else
        ((d['value'] + error.loc[self.stat, 'p0.25<v<=p0.5']) if ((d['value'] > q25) & (d['value'] <= q50)) else
         ((d['value'] + error.loc[self.stat, 'p0.5<v<=p0.75']) if ((d['value'] > q50) & (d['value'] <= q75)) else
          ((d['value'] + error.loc[self.stat, 'p0.75<v<=max']) if ((d['value'] > q75) & (d['value'] <= qmax)) else
           d['value']))), axis=1)

        self.write_json(meta, df)
        return None

    def read_error_json(self):
        df = None
        try:
            with open(self.error_file, 'r') as json_file:
                data = json.load(json_file)
                df = json_normalize(data['error'])
                df = df.set_index('stat')
        except Exception as e:
            print(str(e))
        return df

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

    def write_json(self, meta, df):
        data = {'metaInfo': meta, 'data': df.to_dict('records')}
        with open(self.out_file, 'w') as f:
            json.dump(data, f, indent=4)
        print(self.out_file + '...completed.')
        return None


# target_nwm = r'C:\Users\anuragsrivastav\Desktop\nwm\9731476_nwm_joined.json'
# target_adj = r'C:\Users\anuragsrivastav\Desktop\nwm\9731476_adjusted.json'
# source_error = r'C:\Users\anuragsrivastav\Desktop\nwm\9731264_error.json'
# a = AdjustTS(target_nwm, target_adj, source_error, 'mean')
# a.process()
