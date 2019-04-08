import csv
import os
from concurrent.futures import ThreadPoolExecutor
import requests
import json

dir = os.path.dirname(__file__)


def to_csv(area):
    filename = os.path.join(dir, 'out', area['id'] + '.csv')
    with open(filename, 'w', newline='') as csvfile:
        field_names = [
            'Area Name',
            'Area Id',
            'Area Threshold (cfs)',
            'Forecast Time (UTC)',
            'Forecast Future (UTC)',
            'Streamflow (cfs)'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(format_area(area))


def format_area(area):
    def format_streamflow(streamflow, area):
        return {
            'Area Name': area['name'],
            'Area Id': area['id'],
            'Area Threshold (cfs)': area['threshold'],
            'Forecast Time (UTC)': streamflow['time'],
            'Forecast Future (UTC)': streamflow['hour'],
            'Streamflow (cfs)': streamflow['streamflow']
        }

    return [format_streamflow(streamflow, area) for streamflow in area['forecasts']]


def write_files(areas):
    with ThreadPoolExecutor(max_workers=(len(areas))) as executor:
        for area in areas:
            executor.submit(to_csv, area)


# response = {
#     'json': json.loads(requests.get('https://waterforecast.rtiamanzi.org/api/v1/areas/data',
#                          params={'time': '2019-04-03T10:00:00Z'}).content)
# }
#
# print(response)
# write_files(response['json'])
