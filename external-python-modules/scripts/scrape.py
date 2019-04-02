import bs4
import requests
import datetime


def get_html(url, today):
    formatted_url = url.format(today.strftime("%Y%m%d"))
    return requests.get(formatted_url, headers={'Content-Type': 'text/plain'}).content


def get_today():
    return datetime.datetime.utcnow()


def scrape_html(url, today, forecast, file):
    html = get_html(url, today)
    soup = bs4.BeautifulSoup(html, 'html.parser')
    a_tags = soup.find_all('a')
    fmt_file = file.format(str(forecast).zfill(2))
    find_file = lambda f: f.get_text().startswith(fmt_file)
    file_exists = len(list(filter(find_file, a_tags))) > 0
    print('file {0} is present ?  = {1}'.format(fmt_file, file_exists))
    return file_exists
