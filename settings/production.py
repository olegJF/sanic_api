import os
from databases import Database
from sanic.config import Config

db_url = os.environ.get('SANIC_DB_URL')
settings = Config()
settings.update_config(dict(DEBUG=True, DB_URL=db_url))
