import os
import sys
import time
import datetime
import copy
import json
import pickle
from concurrent.futures import ThreadPoolExecutor

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmbody.agent_base import AgentBase
from pandaatm.atmutils.slow_task_analyzer_utils import get_job_durations, get_jobs_time_consumption_statistics, bad_job_test_main


# parameters
ts_now = datetime.datetime.utcnow()
created_since = ts_now - datetime.timedelta(days=21)
created_before = ts_now
# created_since = datetime.datetime(2020, 9, 1)
# created_before = datetime.datetime(2020, 9, 15)
task_duration = datetime.timedelta(hours=120)
prod_source_label = 'user'


# main
def main():
    dump_file = sys.argv[1]
    # get db proxy
    agent = AgentBase()
    # start
    print('start')
    cand_ret_dict = agent.dbProxy.slowTaskAttemptsFilter01_ATM( created_since=created_since,
                                                                created_before=created_before,
                                                                prod_source_label=prod_source_label,
                                                                task_duration=task_duration)
    ret_dict = {}
    # function to handle one task
    def _handle_one_task(item):
        # start
        k, v = item
        jediTaskID, attemptNr = k
        key_name = '{0}_{1:02}'.format(*k)
        new_v = copy.deepcopy(v)
        # get a dbProxy
        tmp_dbProxy = agent.dbProxyPool.getProxy()
        # call dbProxy
        jobspec_list = tmp_dbProxy.slowTaskJobsInAttempt_ATM(jediTaskID=jediTaskID, attemptNr=attemptNr,
                                                            attempt_start=v['startTime'], attempt_end=v['endTime'])
        # put back dbProxy
        agent.dbProxyPool.putProxy(tmp_dbProxy)
        # time consumption statistics of jobs
        task_attempt_duration = v['attemptDuration']
        jobs_time_consumption_stats_dict = get_jobs_time_consumption_statistics(jobspec_list)
        jobful_time_ratio = jobs_time_consumption_stats_dict['total']['total'] / task_attempt_duration
        successful_run_time_ratio = jobs_time_consumption_stats_dict['finished']['run'] / task_attempt_duration
        jobs_time_consumption_stats_dict['_jobful_time_ratio'] = jobful_time_ratio
        jobs_time_consumption_stats_dict['_successful_run_time_ratio'] = successful_run_time_ratio
        # fill new value dictionary
        new_v['jobs_time_consumption_stats_dict'] = jobs_time_consumption_stats_dict
        # help to release memory
        del jobspec_list
        # return
        return k, new_v
    # parallel run with multithreading
    with ThreadPoolExecutor(4) as thread_pool:
        result_iter = thread_pool.map(_handle_one_task, cand_ret_dict.items())
    # fill ret_dict
    ret_dict.update(result_iter)
    # pickle
    with open(dump_file, 'wb') as _f:
        pickle.dump(ret_dict, _f)
    print('done')


# run
if __name__ == '__main__':
    main()
