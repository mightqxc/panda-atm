import os
import sys
import time
import datetime
import copy
import json
import pickle

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmbody.agent_base import AgentBase
from pandaatm.atmutils.slow_task_analyzer_utils import get_job_durations, get_jobs_time_consumption_statistics, bad_job_test_main


# parameters
created_since = datetime.datetime.utcnow() - datetime.timedelta(days=21)
task_duration = datetime.timedelta(hours=120)


# main
def main():
    dump_file = sys.argv[1]
    # get db proxy
    agent = AgentBase()
    # start
    print('start')
    cand_ret_dict = agent.dbProxy.slowTaskAttempsFilter01_ATM(created_since=created_since, prod_source_label=None, task_duration=task_duration)
    ret_dict = {}
    for k, v in cand_ret_dict.items():
        jediTaskID, attemptNr = k
        task_attempt_name = '{0}_{1:02}'.format(*k)
        new_v = copy.deepcopy(v)
        jobspec_list = agent.dbProxy.slowTaskJobsInAttempt_ATM(jediTaskID=jediTaskID, attemptNr=attemptNr,
                                                            attempt_start=v['startTime'], attempt_end=v['endTime'])
        # time consumption statistics of jobs
        real_task_duration = v['endTime'] - v['startTime']
        jobs_time_consumption_stats_dict = get_jobs_time_consumption_statistics(jobspec_list)
        jobful_time_ratio = jobs_time_consumption_stats_dict['total']['total'] / real_task_duration
        successful_run_time_ratio = jobs_time_consumption_stats_dict['finished']['run'] / real_task_duration
        jobs_time_consumption_stats_dict['_jobful_time_ratio'] = jobful_time_ratio
        jobs_time_consumption_stats_dict['_successful_run_time_ratio'] = successful_run_time_ratio
        # fill new value dictionary
        new_v['real_task_duration'] = real_task_duration
        new_v['jobs_time_consumption_stats_dict'] = jobs_time_consumption_stats_dict
        # help to release memory
        del jobspec_list
        # fill ret_dict
        ret_dict[k] = new_v
    # pickle
    with open(dump_file, 'wb') as _f:
        pickle.dump(ret_dict, _f)
    print('done')


# run
if __name__ == '__main__':
    main()
