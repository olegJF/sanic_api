import aiohttp
import asyncio
import datetime as dt
from urllib.parse import urlparse

from bs4 import BeautifulSoup as BS
from databases import Database
from sqlalchemy import column

from models import channel, guide
from settings import db_url

db = Database(db_url)
today = dt.datetime.today()
tomorrow = today + dt.timedelta(1)
five_days_ago = today - dt.timedelta(5)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:47.0) Gecko/20100101 Firefox/47.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
api_url = 'https://api-production.vipplay.ru/api/v1/channels/program?from={}T00:00:00Z&to={}T23:59:59Z'
nastroykino_url = 'https://www.nastroykino.ru/teleprogram/{}'


async def connect_to_db(*args, **kwargs):
    await db.connect()


async def disconnect_from_db(*args, **kwargs):
    await db.disconnect()


async def get_async_data_from_nastroykino(url: str, channels_dct: dict) -> list:
    pr_list = []
    tomorrow_str = f"tv_{dt.datetime.strftime(tomorrow, '%Y%m%d')}"
    day = today.isoweekday()
    format_url = f'next/#{tomorrow_str}' if day == 7 else f'#{tomorrow_str}'
    # if day == 7 https://www.nastroykino.ru/teleprogram/next/#tv_20210419
    url = url.format(format_url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            html = await response.text()
            bsObj = BS(html, "html.parser")
            teleprogram = bsObj.find('div', attrs={'id': 'teleprogram'})
            day = teleprogram.find('div', attrs={'data-day': tomorrow_str})
            rows = day.find_all('div', attrs={'class': 'channel-column'})
            for row in rows:
                channel_name = row.find('div', attrs={'class': 'channel-name'})
                parsed_url = urlparse(channel_name.a['href'])
                slug = parsed_url.path.replace('/', '')
                if slug in channels_dct:
                    channel_id = channels_dct.get(slug)
                    data = row.find_all('li', attrs={'class': 'p-info'})
                    for d in data:
                        time = d.find('span', attrs={'class': 'time'})
                        time = dt.datetime.strptime(time.text, '%H:%M').time()
                        title = d.find('span', attrs={'class': 'text'})
                        title = title.text.replace("subs", "")
                        descr = d.find_all('p', attrs={'class': 'popup-text'})
                        d_text = ''
                        if descr:
                            d_text += ' '.join(p.text for p in descr)
                            d_text = d_text.replace(' ', ' ')
                        description = d_text
                        short = d_text
                        if short and len(short) > 200:
                            short = short[:200]
                            whitespace = short.rfind(' ')
                            short = short[:whitespace]
                        tmp = {"date": tomorrow.date(),
                               "time": time,
                               "description": description,
                               "title": title,
                               "short": short,
                               "channel_id": channel_id}
                        pr_list.append(tmp)
            return pr_list


async def get_acync_data_by_api(api_url: str, channels_dct: dict) -> list:
    url = api_url.format(tomorrow.date().isoformat(), tomorrow.date().isoformat())
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            program_list = await response.json(content_type=None)
            pr_list = []
            for channel in program_list:
                slug = channel['slug']
                if slug in channels_dct:
                    channel_id = channels_dct.get(slug)
                    programs = channel['programs']
                    for pr in programs:
                        start_time = pr['start_time'].split('.')[0]
                        start_time = dt.datetime.fromisoformat(start_time)
                        short = pr['description']
                        if short and len(short) > 200:
                            short = short[:200]
                            whitespace = short.rfind(' ')
                            short = short[:whitespace]
                        tmp = {"date": start_time.date(),
                               "time": start_time.time(),
                               "description": pr['synopsis_this_episode'],
                               "title": pr['title'],
                               "short": short,
                               "channel_id": channel_id}
                        pr_list.append(tmp)
            return pr_list


async def get_channels_dct():
    # query = channel.select().filter(column('slug').in_(channels_slug))
    query = channel.select()
    query = str(query.compile(compile_kwargs={"literal_binds": True}))
    rows = await db.fetch_all(query=query)
    _channels_dct = {}
    for ch in rows:
        _channels_dct[ch.get('slug')] = ch.get('id')
    return _channels_dct


async def create_guide_records(rows):
    lst = []
    for row in rows:
        query = guide.insert().values(**row)
        lst.append(await db.execute(query))
    return lst


async def delete_old_guide_records():
    query = guide.delete().where(column('date') <= five_days_ago.date())
    tmp = await db.execute(query)


async def get_data(n_kino_url: str, api_url: str, channels_dct: dict) -> list:
    lst = []
    futures = [
        get_async_data_from_nastroykino(n_kino_url, channels_dct),
        get_acync_data_by_api(api_url, channels_dct),
    ]
    for future in asyncio.as_completed(futures):
        lst += await future
    return lst


async def main():
    await connect_to_db(db)
    channels_dct = await get_channels_dct()
    result = await get_data(nastroykino_url, api_url, channels_dct)
    guides = await create_guide_records(result)
    print(f"created {len(guides)} records")
    # deleting old data from DB
    await delete_old_guide_records()
    await disconnect_from_db()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
