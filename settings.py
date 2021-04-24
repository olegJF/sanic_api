from sanic.config import Config
from environs import Env
from databases import Database

env = Env()
env.read_env('.env')
# DEBUG = env.bool("DEBUG", default=False)

settings = Config()
settings.load_environment_vars('SANIC_')
db_url = settings.get('DB_URL')
# user = settings.get('DB_USER')
# host = settings.get('DB_HOST')
# name = settings.get('DB_NAME')
# password = settings.get('DB_PASSWORD')
# db_url = f'postgresql://{user}:{password}@{host}/{name}'

db = Database(db_url)
