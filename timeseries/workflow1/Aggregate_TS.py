# process to aggregate a time series.
# in: json file, aggregate criteria
# out: gage_id.JSON

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


class AggregateTS(object):

    meta_info = None
    ts_data = None

    def __init__(self, in_file, out_file, duration, method):
        self.in_file = in_file
        self.out_file = out_file
        self.duration = duration
        self.method = method

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
        if df is not None:
            data = {'metaInfo': self.meta_info, 'data': df.to_dict('records')}
            with open(self.out_file, 'w') as f:
                json.dump(data, f, indent=4)
        print(self.out_file + '...completed.')
        return None

    def process(self):
        df = self.read_json()
        if df is not None:
            if self.method == 'mean':
                if self.duration == 'daily':
                    df = df.resample('D', on='date').mean()
                    df = df.reset_index()

                    df['index'] = df['date']
                    df = df.set_index('index')
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

                    self.write_json(df)
        return None
