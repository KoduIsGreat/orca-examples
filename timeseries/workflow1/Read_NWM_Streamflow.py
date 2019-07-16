# process to downloaded NWM csv file to extract the data for a COMID.
# in: csv file, com_id
# out: com_id.JSON

# author: anuragsrivastav@rti.org
# date: April 08, 2019
# version: 1.0
# revision notes:
#
#
#
#

import json
import pandas as pd


class Streamflow(object):

    def __init__(self, com_id, in_file, out_file):
        self.com_id = com_id
        self.in_file = in_file
        self.out_file = out_file

    def get_time_series(self):
        df = pd.read_csv(self.in_file, index_col=False)
        df = df[['time', self.com_id]]
        df = df.rename(columns={'time': 'date', self.com_id: 'value'})
        df['index'] = df['date']
        df = df.set_index('index')
        return df.to_dict('records')

    def prepare_metadata(self):
        metadata = {'type': 'org.rti.wrmd.commons.timeseries.TimeSeries',
                    'gageId': self.com_id,
                    'timeZoneInfo': None,
                    'parameter': 'streamflow',
                    'forecastTime': None,
                    'interval': 'hourly',
                    'geoLocation': {'types': None, 'coordinate': []}
                    }
        return metadata

    def get_data(self):
        data = {'metaInfo': self.prepare_metadata(), 'data': self.get_time_series()}
        with open(self.out_file, 'w') as f:
            json.dump(data, f, indent=4)
        print(self.com_id + '...completed.')
        return None
