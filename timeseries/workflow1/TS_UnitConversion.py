# process to convert time-series units
# in: json file, conversion_multiplier
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


class TSConvertUnits(object):

    def __init__(self, in_file, multiplier, out_file):
        self.in_file = in_file
        self.multiplier = multiplier
        self.out_file = out_file

    def process(self):
        [meta, df] = self.read_json(self.in_file)

        df['value'] = df['value'].apply(lambda x: float(x)*float(self.multiplier))
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        self.write_json(meta, df, self.out_file)

        print('unit conversion for {0}...completed.'.format(self.in_file))
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
