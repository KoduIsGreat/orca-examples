# process to fill the time series gaps.
# in: json file, fill criteria
# out: gage_id.JSON

# author: anuragsrivastav@rti.org
# date: April 03, 2019
# version: 1.0
# revision notes:
#
#
#
# reference: https://medium.com/@drnesr/filling-gaps-of-a-time-series-using-python-d4bfddd8c460

import json
import pandas as pd
from pandas.io.json import json_normalize


class FillTS(object):

    meta_info = None
    ts_data = None

    def __init__(self, in_file, out_file, criteria, window=None, interp_type=None):
        self.in_file = in_file
        self.out_file = out_file
        self.criteria = criteria
        self.window = window
        self.interp_type = interp_type

    def process(self):
        df = self.read_json()
        if df is not None:
            if self.criteria == 'mean':
                df = self.fill_gap_mean(df)
            if self.criteria == 'median':
                df = self.fill_gap_mean(df)

            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        self.write_json(df)
        return None

    def read_json(self):
        df = None
        try:
            with open(self.in_file) as json_file:
                data = json.load(json_file)
                self.meta_info = data['metaInfo']
                df = json_normalize(data['data'])

                # convert the column to DateTime format
                df['date'] = pd.to_datetime(df.date)
                df['index'] = df['date']
                df = df.set_index('index')
        except Exception as e:
            print(str(e))
        return df

    def write_json(self, df):
        data = {'metaInfo': self.meta_info, 'data': df.to_dict('records')}
        with open(self.out_file, 'w') as f:
            json.dump(data, f, indent=4)
        print(self.out_file + '...completed.')
        return None

    @staticmethod
    def fill_gap_mean(df):
        if df is not None:
            df = df.assign(value=df.value.fillna(df.value.mean()))
        return df

    @staticmethod
    def fill_gap_median(df):
        if df is not None:
            df = df.assign(value=df.value.fillna(df.value.median()))
        return df




