from pandaserver.taskbuffer import DBProxyPool as panda_db_proxy_pool

# use customized proxy
from . import db_proxy
panda_db_proxy_pool.DBProxy = db_proxy

class DBProxyPool(panda_db_proxy_pool.DBProxyPool):

    # constructor
    def __init__(self, dbhost, dbpasswd, nConnection, useTimeout=False):
        super().__init__(dbhost, dbpasswd, nConnection, useTimeout)

    # get a DBProxyObj containing a proxy
    def get(self):
        proxy_obj = DBProxyObj(db_proxy_pool=self)
        return proxy_obj


# object of context manager for db proxy
class DBProxyObj(object):
    # constructor
    def __init__(self, db_proxy_pool):
        self.proxy_pool = db_proxy_pool
        self.proxy = None

    # get proxy
    def __enter__(self):
        self.proxy = self.proxy_pool.getProxy()
        return self.proxy

    # release proxy
    def __exit__(self, type, value, traceback):
        self.proxy_pool.putProxy(self.proxy)
        self.proxy = None
