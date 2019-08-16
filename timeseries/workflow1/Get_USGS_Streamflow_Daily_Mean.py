# process to download the daily (mean) streamflow data for a USGS gage.
# in: gage id, start date, end date, destination path
# out: gage_id.JSON

# author: anuragsrivastav@rti.org
# date: April 02, 2019
# version: 1.0
# revision notes:
#
#
#
#

import os
import requests
import json


class Streamflow(object):

    URL = 'https://waterservices.usgs.gov/nwis/dv/?sites={0}&startDT={1}&endDT={2}&format=rdb&parameterCd=00060&' \
          'statCd=00003&siteStatus=All'

    def __init__(self, gage_id, start_date, end_date, out_file):
        self.gage_id = gage_id
        self.start_date = start_date
        self.end_date = end_date
        self.file = out_file

    def get_time_series(self):
        values = []
        url = self.URL.format(self.gage_id, self.start_date, self.end_date)
        web = requests.get(url)
        if web.text:
            for line in web.text.split('\n'):
                if len(line) > 0 and not line[0] == '#':
                    row = line.split('\t')
                    if row[0] == 'USGS':
                            values.append({'date': row[2], 'value': float(row[3])})
        return values

    def prepare_metadata(self):
        metadata = {'type': 'org.rti.wrmd.commons.timeseries.TimeSeries',
                    'gageId': self.gage_id,
                    'timeZoneInfo': None,
                    'parameter': 'streamflow',
                    'forecastTime': None,
                    'interval': 'daily mean',
                    'geoLocation': {'types': None, 'coordinate': []}
                    }
        return metadata

    def get_data(self):
        data = {'metaInfo': self.prepare_metadata(), 'data': self.get_time_series()}
        with open(self.file, 'w') as f:
            json.dump(data, f, indent=4)
        print(self.gage_id + '...completed.')
        return None
