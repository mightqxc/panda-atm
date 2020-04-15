from pandaserver.taskbuffer import DBProxyPool as panda_db_proxy_pool

# use customized proxy
from . import db_proxy
panda_db_proxy_pool.DBProxy = db_proxy

class DBProxyPool(panda_db_proxy_pool.DBProxyPool):

    # constructor
    def __init__(self, dbhost, dbpasswd, nConnection, useTimeout=False):
        super().__init__(dbhost, dbpasswd, nConnection, useTimeout)
