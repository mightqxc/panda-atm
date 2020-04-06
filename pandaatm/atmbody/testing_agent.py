import time
import datetime

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmcore.db_proxy_pool import DBProxyPool





base_logger = logger_utils.setup_logger('testing_agent')


class TestingAgent(object):

    def __init__(self):
        self.sleepPeriod = 300
        self.dbProxyPool = DBProxyPool(atm_config.db.dbhost, atm_config.db.dbpasswd, nConnection=1)
        self.dbProxy = self.dbProxyPool.getProxy()

    def run(self):
        tmp_log = logger_utils.make_logger(base_logger, method_name='TestingAgent.run')
        while True:
            # slow task attemps
            result_str_line_template = '{jediTaskID:>10}  {attemptNr:>4} | {finalStatus:>10} {startTime:>20}  {endTime:>20}  {attemptDuration:>15}'
            result_str_list = []
            result_str_list.append(result_str_line_template.format(jediTaskID='taskID', attemptNr='#N', finalStatus='status', startTime='startTime', endTime='endTime', attemptDuration='duration'))
            created_since = datetime.datetime.utcnow() - datetime.timedelta(days=10)
            retDict = self.dbProxy.slowTaskAttempsFilter01_ATM(created_since)
            for k in sorted(retDict):
                v = retDict[k]
                result_str_line = result_str_line_template.format(jediTaskID=k[0], attemptNr=k[1], finalStatus=v['finalStatus'],
                                                                    startTime=v['startTime'].strftime("%y-%m-%d %H:%M:%S"),
                                                                    endTime=v['endTime'].strftime("%y-%m-%d %H:%M:%S"),
                                                                    attemptDuration=core_utils.timedelta_parse_dict(v['attemptDuration'])['str_dcolon'])
                result_str_list.append(result_str_line)
            tmp_log.debug('slow task attemps: \n{0}\n'.format('\n'.join(result_str_list)))
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
