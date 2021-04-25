import sys

from .production import *
try:
    from .local_settings import *
except ImportError:
    pass
db = Database(db_url)
