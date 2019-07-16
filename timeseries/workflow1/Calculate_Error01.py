# process to calculate the error statistics by taking the difference between observed and simulated time-series
# in: TS_observed (json), TS_simulated (json)
# out: error statistics (json)

# author: anuragsrivastav@rti.org
# date: April 15, 2019
# version: 1.0
# revision notes:

#
#
#

import json
import pandas as pd
from pandas.io.json import json_normalize


class ErrorStats(object):

    def __init__(self, in_file1, in_file2, out_file):
        self.in_file1 = in_file1
        self.in_file2 = in_file2
        self.out_file = out_file

    def process(self):
        obs = self.read_json(self.in_file1)[1]
        sim = self.read_json(self.in_file2)[1]

        df = pd.merge(obs, sim, on='date', how='inner')
        d1 = self.calc_data_stats(df, 'value_x')
        d2 = self.calc_data_stats(df, 'value_y')
        s = self.calc_error_stats(df)
        print(s)

        self.write_json(d1, d2, s)
        return None

    @staticmethod
    def prepare_metadata():
        metadata = {'type': None,
                    'gageId': None,
                    'timeZoneInfo': None,
                    'parameter': 'error statistics',
                    'forecastTime': None,
                    'interval': None,
                    'geoLocation': {'types': None, 'coordinate': []}
                    }
        return metadata

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
        except Exception as err:
            print(str(err))
        return [meta_info, df]

    def write_json(self, data1_df, data2_df, error_df):
        data = {'metaInfo': self.prepare_metadata(), 'data1': data1_df.to_dict('records'),
                'data2': data2_df.to_dict('records'), 'error': error_df.to_dict('records')}
        with open(self.out_file, 'w') as f:
            json.dump(data, f, indent=4)
        print(self.out_file + '...completed.')
        return None

    @staticmethod
    def calc_data_stats(df, field):
        stats = []
        if df is not None:
            stats.append(['min', df[field].min()])
            stats.append(['q0.25', df[field].quantile(0.25)])
            stats.append(['mean', df[field].mean()])
            stats.append(['median', df[field].median()])
            stats.append(['q0.75', df[field].quantile(0.75)])
            stats.append(['max', df[field].max()])
            stats.append(['std', df[field].std()])
        stat_df = pd.DataFrame(stats, columns=['stat', 'value'])
        return stat_df

    def calc_error_stats(self, df):
        stats = self.error_stats(df)
        stats = stats.rename(columns={'value': 'min<=v<=max'})

        stats['v<=p0.25'] = self.error_stats(df[df['value_x'] <= df['value_x'].quantile(0.25)])['value']

        temp = df[(df['value_x'].quantile(0.25) < df['value_x']) & (df['value_x'] <= df['value_x'].quantile(0.5))]
        stats['p0.25<v<=p0.5'] = self.error_stats(temp)['value']

        temp = df[(df['value_x'].quantile(0.5) < df['value_x']) & (df['value_x'] <= df['value_x'].quantile(0.75))]
        stats['p0.5<v<=p0.75'] = self.error_stats(temp)['value']

        temp = df[(df['value_x'].quantile(0.75) < df['value_x']) & (df['value_x'] <= df['value_x'].max())]
        stats['p0.75<v<=max'] = self.error_stats(temp)['value']
        return stats

    @staticmethod
    def error_stats(df):
        stats = []
        if df is not None:
            error = df['value_x'] - df['value_y']
            stats.append(['min', error.min()])
            stats.append(['q0.25', error.quantile(0.25)])
            stats.append(['mean', error.mean()])
            stats.append(['median', error.median()])
            stats.append(['q0.75', error.quantile(0.75)])
            stats.append(['max', error.max()])
            stats.append(['std', error.std()])
        stat_df = pd.DataFrame(stats, columns=['stat', 'value'])
        return stat_df


# f1 = r'C:\Users\anuragsrivastav\Desktop\nwm\9731264_nwm_joined.json'
# f2 = r'C:\Users\anuragsrivastav\Desktop\nwm\02146285_usgs_joined.json'
# o = r'C:\Users\anuragsrivastav\Desktop\nwm\9731264_02146285_error.json'
# e = ErrorStats(f1, f2, o)
# e.process()


