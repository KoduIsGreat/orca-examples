import bs4
import requests
import datetime

today = datetime.datetime.utcnow()
forecast = 0
file = 'nwm.t{0}z.short_range.channel_rt'
url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{0}/short_range/'
formatted_url = url.format(today.strftime("%Y%m%d"))
html = requests.get(formatted_url, headers={'Content-Type': 'text/plain'}).content
soup = bs4.BeautifulSoup(html, 'html.parser')
a_tags = soup.find_all('a')
fmt_file = file.format(str(forecast).zfill(2))
find_file = lambda f: f.get_text().startswith(fmt_file)
file_exists = len(list(filter(find_file, a_tags))) > 0
print('file {0} is present ?  = {1}'.format(fmt_file, file_exists))
