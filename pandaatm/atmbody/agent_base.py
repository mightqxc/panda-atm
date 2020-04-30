import time
import datetime

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmcore.db_proxy_pool import DBProxyPool


class AgentBase(object):

    def __init__(self, **kwargs):
        self.sleepPeriod = 300
        self.dbProxyPool = DBProxyPool(atm_config.db.dbhost, atm_config.db.dbpasswd, nConnection=5)
        self.dbProxy = self.dbProxyPool.getProxy()

    def run(self):
        pass
