import time
import datetime

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmbody.agent_base import AgentBase





base_logger = logger_utils.setup_logger('testing_agent')


class TestingAgent(AgentBase):

    def __init__(self):
        super().__init__()
        self.sleepPeriod = 10

    def run(self):
        tmp_log = logger_utils.make_logger(base_logger, method_name='TestingAgent.run')
        while True:
            # do something
            tmp_log.debug('start')
            # sleep
            time.sleep(self.sleepPeriod)




# launch
def launcher():
    tmp_log = logger_utils.make_logger(base_logger, method_name='launcher')
    tmp_log.debug('start')
    try:
        pass
    except Exception as e:
        # tmp_log.error('failed to read config json file; should not happen... {0}: {1}'.format(e.__class__.__name__, e))
        raise e
    else:
        agent = TestingAgent()
        agent.run()
