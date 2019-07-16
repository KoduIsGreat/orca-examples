# process to calculate skill score
# in: obs file (json), sim file (json)
# out: json file

# author: anuragsrivastav@rti.org
# date: April 23, 2019
# version: 1.0
# revision notes:
#
#
#

import json
import pandas as pd
from pandas.io.json import json_normalize


class SkillScores(object):

    def __init__(self, obs_file, sim_file, out_file):
        self.obs_file = obs_file
        self.sim_file = sim_file
        self.out_file = out_file

    def process(self):
        obs = self.read_json(self.obs_file)[1]
        sim = self.read_json(self.sim_file)[1]

        df = pd.merge(obs, sim, on='date', how='inner')
        scores = self.calc_scores(df)
        print(scores)

        self.write_json(scores)
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
    def calc_scores(df):
        score = []
        if df is not None:
            error = df['value_x'] - df['value_y']
            score.append(['Bias', error.mean()])
            score.append(['MAE', (error.abs()).mean()])
            score.append(['MSE', (error ** 2).mean()])
            score.append(['RMSE', (error ** 2).mean() ** .5])
        score_df = pd.DataFrame(score, columns=['score', 'value'])
        return score_df

    @staticmethod
    def prepare_metadata():
        metadata = {'type': None,
                    'gageId': None,
                    'timeZoneInfo': None,
                    'parameter': 'skills scores',
                    'forecastTime': None,
                    'interval': None,
                    'geoLocation': {'types': None, 'coordinate': []}
                    }
        return metadata

    def write_json(self, score_df):
        data = {'metaInfo': self.prepare_metadata(), 'data': score_df.to_dict('records')}
        with open(self.out_file, 'w') as f:
            json.dump(data, f, indent=4)
        print(self.out_file + '...completed.')
        return None

# f1 = r'C:\Users\anuragsrivastav\Desktop\nwm\0214627970_usgs_joined.json'
# f2 = r'C:\Users\anuragsrivastav\Desktop\nwm\9731476_nwm_joined.json'
# o = r'C:\Users\anuragsrivastav\Desktop\nwm\9731476_scores.json'
# s = SkillScores(f1, f2, o)
# s.process()
