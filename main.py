import datetime as dt

from sanic import Sanic, response
from sanic.request import Request
from sanic.response import json

from models import Channel, Guide, GuideModel
from settings import settings, db

app = Sanic('TV-Guide')
app.config.update_config(settings)



def serialize_dict(dct: dict) -> dict:
    tmp = {}
    for key, val in dct.items():
        if isinstance(val, (dt.date, dt.time, dt.datetime)):
            val = val.isoformat()
        tmp[key] = val
    return tmp


def setup_database():
    # app.db = Database(app.config.DB_URL)
    app.db = db

    @app.listener('after_server_start')
    async def connect_to_db(*args, **kwargs):
        await app.db.connect()

    @app.listener('after_server_stop')
    async def disconnect_from_db(*args, **kwargs):
        await app.db.disconnect()


@app.route("/")
async def home(request):
    return response.html('<h1>Hello Sanic!</h1>')


@app.post("/channels/")
async def create_channel(request: Request):
    dct = request.json
    channel_id = await Channel.create(**dct)
    return json({"channel_id": channel_id}, status=201)


@app.route("/channels/<pk>/")
async def get_channel(request: Request, pk: int):
    pk = int(pk)
    channel = await Channel.get(pk)
    return json(body={**channel}, status=200)


@app.route("/channels/slug/<slug>/")
async def get_channel_by_slug(request: Request, slug: str):
    channel = await Channel.get_by_slug(slug)
    return json(body={**channel}, status=200)


@app.get("/channels/")
async def get_channels(request: Request):
    channels = await Channel.get_list()
    rows = [{**channel} for channel in channels]
    return json(rows, status=200)


@app.post("/guides/")
async def create_guides(request: Request):
    lst = request.json
    _tmp = []
    for dct in lst:
        g = GuideModel.parse_obj(dct)
        _tmp.append(g)
    guides = await Guide.create_many(_tmp)
    return json(body=guides, status=201)


# @app.route("/guides/<pk>/")
# async def get_guides_by_channel_id(request: Request, pk: int):
#     pk = int(pk)
#     guides = await Guide.get_list_by_channel_id(pk)
#     lst = []
#     for row in guides:
#         g = GuideModel.parse_obj(row)
#         tmp = serialize_dict(g.dict(exclude={'id'}))
#         lst.append(tmp)
#     return json(body=lst, status=200)
#

@app.route("/guides/<slug>/")
async def get_guides_by_channel(request: Request, slug: str):
    today = dt.datetime.today()
    date = today.date()
    guides = await Guide.get_list_by_channel_slug(slug, date)
    lst = []
    for row in guides:
        g = GuideModel.parse_obj(row)
        tmp = serialize_dict(g.dict(exclude={'id'}))
        lst.append(tmp)
    return json(body=lst, status=200)

setup_database()

if __name__ == '__main__':
    # app.config.update_config(settings)
    # setup_database()
    app.run(
        host=app.config.HOST,
        port=app.config.PORT,
        debug=app.config.DEBUG,
        auto_reload=app.config.DEBUG,
    )
