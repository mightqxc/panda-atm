import os
import sys
import time
import datetime
import copy
import json
import pickle
import threading
from concurrent.futures import ThreadPoolExecutor

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmbody.agent_base import AgentBase
from pandaatm.atmutils.slow_task_analyzer_utils import get_total_jobs_run_core_time, get_task_attempts_in_each_duration


# parameters
created_since = datetime.datetime.datetime(2020, 4, 10, 0, 0, 0)
created_before = datetime.datetime.datetime(2020, 4, 24, 0, 0, 0)
prod_source_label = 'user'
gshare = 'User Analysis'
task_duration = datetime.timedelta(seconds=1)
cores_per_user = 100


# main
def main():
    dump_file = sys.argv[1]
    # get db proxy
    agent = AgentBase()
    # start
    print('start')
    cand_ret_dict = agent.dbProxy.slowTaskAttempsFilter01_ATM(
                                        created_since=created_since,
                                        created_before=created_before,
                                        prod_source_label=prod_source_label,
                                        gshare=gshare,
                                        task_duration=task_duration)
    # global lock
    global_lock = threading.Lock()
    # task attempts by user
    user_task_attemps_map = {}
    # function to handle one task
    def _handle_one_task(item):
        # start
        k, v = item
        jediTaskID, attemptNr = k
        task_attempt_name = '{0}_{1:02}'.format(*k)
        new_v = copy.deepcopy(v)
        attempt_duration = v['attemptDuration']
        user_name = v['userName']
        # get jobs
        with agent.dbProxyPool.get() as proxy:
            jobspec_list = proxy.slowTaskJobsInAttempt_ATM(jediTaskID=jediTaskID, attemptNr=attemptNr,
                                                                attempt_start=v['startTime'], attempt_end=v['endTime'])
        # run-wait of the task attempt
        run_core_time = get_total_jobs_run_core_time(jobspec_list)
        user_run_time = run_core_time/cores_per_user
        user_wait_time = attempt_duration - user_run_time
        user_run_wait_dict = {
                'user_run_time': user_run_time,
                'user_wait_time': user_wait_time,
                'user_run_proportion': user_run_time/attempt_duration,
                'user_wait_proportion': user_wait_time/attempt_duration,
            }
        # fill new value dictionary
        new_v.update(user_run_wait_dict)
        # help to release memory
        del jobspec_list
        # put into map
        with global_lock:
            if user_name in user_task_attemps_map:
                user_task_attemps_map[user_name].update({k: new_v})
            else:
                user_task_attemps_map[user_name] = {k: new_v}
    # parallel run with multithreading
    with ThreadPoolExecutor(4) as thread_pool:
        result_iter = thread_pool.map(_handle_one_task, cand_ret_dict.items())
    # function to handle task attemps for one user (handle overlap)
    def _handle_one_user(item):
        user_name, task_attempt_dict = item
        (   duration_list,
            n_tasks_in_duration_list,
            task_attempts_in_duration_list) = get_task_attempts_in_each_duration(task_attempt_dict)
        # make equivalent task attempts
        user_total_taskful_time = datetime.timedelta()
        user_total_run_time = datetime.timedelta()
        user_total_wait_time = datetime.timedelta()
        for duration, key_set in zip(duration_list, task_attempts_in_duration_list):
            n_task_atttempts = len(key_set)
            if n_task_atttempts > 0:
                for key in key_set:
                    if task_attempt_dict[key]['attemptDuration']:
                        ratio = duration/task_attempt_dict[key]['attemptDuration']
                        user_total_taskful_time += duration
                        user_total_run_time += task_attempt_dict[key]['user_run_time']*ratio
                        user_total_wait_time += task_attempt_dict[key]['user_wait_time']*ratio
        user_total_run_proportion = user_total_run_time/user_total_taskful_time
        user_total_wait_proportion = user_total_wait_time/user_total_taskful_time
        total_dict = {
                'user_total_taskful_time': user_total_taskful_time,
                'user_total_run_time': user_total_run_time,
                'user_total_wait_time': user_total_wait_time,
                'user_total_run_proportion': user_total_run_proportion,
                'user_total_wait_proportion': user_total_wait_proportion,
            }
        # return
        return (user_name, total_dict)
    # compute run-wait for users
    user_run_wait_map = {}
    with ThreadPoolExecutor(4) as thread_pool:
        result_iter = thread_pool.map(_handle_one_user, user_task_attemps_map.items())
    user_run_wait_map.update(result_iter)
    # print
    print(user_run_wait_map)
    # pickle
    with open(dump_file, 'wb') as _f:
        pickle.dump(user_run_wait_map, _f)
    print('done')


# run
if __name__ == '__main__':
    main()
