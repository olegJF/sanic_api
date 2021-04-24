from datetime import date, time

import sqlalchemy
from pydantic import BaseModel, constr
from sqlalchemy import column
from sqlalchemy.ext.declarative import declarative_base

from settings import db

Base = declarative_base()

metadata = sqlalchemy.MetaData()

channel = sqlalchemy.Table(
    'channel',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('name', sqlalchemy.String(length=100), nullable=False),
    sqlalchemy.Column('slug', sqlalchemy.String(length=100), nullable=False),
)

guide = sqlalchemy.Table(
    'guide',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('date', sqlalchemy.Date, nullable=False),
    sqlalchemy.Column('time', sqlalchemy.Time, nullable=False),
    sqlalchemy.Column('description', sqlalchemy.Text, nullable=True),
    sqlalchemy.Column('short', sqlalchemy.String(length=200), nullable=False),
    sqlalchemy.Column('title', sqlalchemy.String(length=200), nullable=False),
    sqlalchemy.Column('channel_id', sqlalchemy.Integer,
                      sqlalchemy.ForeignKey('channel.id'), nullable=False)
)


class ChannelModel(BaseModel):
    id: int = None
    name: constr(max_length=100)
    slug: constr(max_length=100)

    class Config:
        orm_mode = True


class GuideModel(BaseModel):
    id: int = None
    date: date
    time: time
    title: constr(max_length=200)
    description: constr()
    short: constr(max_length=200)
    channel_id: int

    class Config:
        orm_mode = True


class Channel:

    def __init__(self, _db, *args, **kwargs):
        self.db = _db

    @classmethod
    async def get(cls, pk):
        query = channel.select().where(channel.c.id == pk)
        _channel = await db.fetch_one(query)
        return _channel

    @classmethod
    async def get_by_slug(cls, slug):
        query = channel.select().where(column('slug') == slug)
        _channel = await db.fetch_one(query)
        return _channel

    # async def get_dict_by_slug_list(self, slug_list):
    #     dct = {}
    #     query = channel.select().where(channel.c.slug in slug_list)
    #     _channel = await self.db.fetch_all(query)
    #     for ch in _channel:
    #         dct[ch.get('slug')] = ch.get('id')
    #     return dct

    @classmethod
    async def get_list(cls):
        query = channel.select()
        _channels = await db.fetch_all(query)
        return _channels

    @classmethod
    async def create(cls, **dct):
        query = channel.insert().values(**dct)
        channel_id = await db.execute(query)
        return channel_id


class Guide:
    @classmethod
    async def get(cls, pk):
        query = guide.select().where(guide.c.id == pk)
        _guide = await db.fetch_one(query)
        return _guide

    @classmethod
    async def get_list_by_channel_id(cls, channel_id):
        query = guide.select().where(
            column('channel_id') == channel_id).order_by('date', 'time')
        _guides = await db.fetch_all(query)
        return _guides

    @classmethod
    async def get_list_by_channel_slug(cls, slug, _date):
        _guides = []
        query = channel.select().where(column('slug') == slug)
        _channel = await db.fetch_one(query)
        if _channel:
            channel_id = _channel.get('id')
            if channel_id:
                query = guide.select().where(
                    column('channel_id') == channel_id).where(
                    column('date') == _date).order_by('date', 'time')
                _guides = await db.fetch_all(query)
        return _guides

    @classmethod
    async def create_many(cls, values):
        lst = []
        for row in values:
            dct = row.dict(exclude={'id'})
            query = guide.insert().values(**dct)
            lst.append(await db.execute(query))
        return lst

    @classmethod
    async def create_many_records(cls, values):
        lst = []
        for row in values:
            query = guide.insert().values(**row)
            lst.append(await db.execute(query))
        return lst
