import os
from databases import Database

db_url = os.environ.get('SANIC_DB_URL')

settings = dict(DEBUG=False)
