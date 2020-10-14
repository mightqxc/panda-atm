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
created_since = datetime.datetime(2020, 4, 25, 0, 0, 0)
created_before = datetime.datetime(2020, 4, 30, 0, 0, 0)
prod_source_label = 'user'
gshare = 'User Analysis'
task_duration = datetime.timedelta(hours=1)
cores_per_user = 100


# main
def main():
    dump_file = sys.argv[1]
    # get db proxy
    agent = AgentBase()
    # start
    print('start')
    # checkpoint file
    cand_ret_dict_file = '/tmp/user_run_wait-cand_ret_dict.pickle'
    try:
        with open(cand_ret_dict_file, 'rb') as _f:
            cand_ret_dict = pickle.load(_f)
    except FileNotFoundError:
        cand_ret_dict = agent.dbProxy.slowTaskAttemptsFilter01_ATM(
                                            created_since=created_since,
                                            created_before=created_before,
                                            prod_source_label=prod_source_label,
                                            gshare=gshare,
                                            task_duration=task_duration)
        # pickle for checkpoint
        with open(cand_ret_dict_file, 'wb') as _f:
            pickle.dump(cand_ret_dict, _f)
    # global lock
    global_lock = threading.Lock()
    # task attempts by user
    user_task_attempts_map = {}
    # function to handle one task
    def _handle_one_task(item):
        # start
        k, v = item
        jediTaskID, attemptNr = k
        key_name = '{0}_{1:02}'.format(*k)
        new_v = copy.deepcopy(v)
        attempt_duration = v['attemptDuration']
        user_name = v['userName']
        # get jobs
        with agent.dbProxyPool.get() as proxy:
            jobspec_list = proxy.slowTaskJobsInAttempt_ATM(jediTaskID=jediTaskID, attemptNr=attemptNr,
                                                                attempt_start=v['startTime'], attempt_end=v['endTime'],
                                                                concise=True)
        # run cputime of jobs of the task attempt
        run_core_time, successful_run_core_time = get_total_jobs_run_core_time(jobspec_list)
        user_jobs_run_time_dict = {
                'user_jobs': len(jobspec_list),
                'user_run_cputime': run_core_time,
                'user_successful_run_cputime': successful_run_core_time,
            }
        # fill new value dictionary
        new_v.update(user_jobs_run_time_dict)
        # help to release memory
        del jobspec_list
        # put into map
        with global_lock:
            if user_name in user_task_attempts_map:
                user_task_attempts_map[user_name].update({k: new_v})
            else:
                user_task_attempts_map[user_name] = {k: new_v}
    # checkpoint file
    user_task_attempts_map_file = '/tmp/user_run_wait-user_task_attempts_map.pickle'
    try:
        with open(user_task_attempts_map_file, 'rb') as _f:
            user_task_attempts_map = pickle.load(_f)
    except FileNotFoundError:
        # parallel run with multithreading
        with ThreadPoolExecutor(4) as thread_pool:
            result_iter = thread_pool.map(_handle_one_task, cand_ret_dict.items())
        # pickle for checkpoint
        with open(user_task_attempts_map_file, 'wb') as _f:
            pickle.dump(user_task_attempts_map, _f)
    # help to release memory
    del cand_ret_dict
    # function to handle task attempts for one user (handle overlap)
    def _handle_one_user(item):
        user_name, task_attempt_dict = item
        (   duration_list,
            n_tasks_in_duration_list,
            task_attempts_in_duration_list) = get_task_attempts_in_each_duration(task_attempt_dict)
        # make equivalent task attempts
        total_taskful_time = datetime.timedelta()
        total_run_time = datetime.timedelta()
        total_successful_run_time = datetime.timedelta()
        total_wait_time = datetime.timedelta()
        for duration, key_set in zip(duration_list, task_attempts_in_duration_list):
            n_task_atttempts = len(key_set)
            if n_task_atttempts > 0:
                total_taskful_time += duration
                for key in key_set:
                    if task_attempt_dict[key]['attemptDuration']:
                        duration_ratio = duration/task_attempt_dict[key]['attemptDuration']
                        total_run_time += task_attempt_dict[key]['user_run_cputime']*duration_ratio/cores_per_user
                        total_successful_run_time += task_attempt_dict[key]['user_successful_run_cputime']*duration_ratio/cores_per_user
        total_wait_time = total_taskful_time - total_run_time
        total_run_proportion = total_run_time/total_taskful_time
        total_successful_run_proportion = total_successful_run_time/total_taskful_time
        total_wait_proportion = total_wait_time/total_taskful_time
        total_jobs = sum(( x['user_jobs'] for x in task_attempt_dict.values() ))
        total_task_attempts = len(task_attempt_dict)
        total_dict = {
                'total_jobs': total_jobs,
                'total_task_attempts': total_task_attempts,
                'total_taskful_time': total_taskful_time,
                'total_run_time': total_run_time,
                'total_successful_run_time': total_successful_run_time,
                'total_wait_time': total_wait_time,
                'total_run_proportion': total_run_proportion,
                'total_successful_run_proportion': total_successful_run_proportion,
                'total_wait_proportion': total_wait_proportion,
            }
        # return
        return (user_name, total_dict)
    # compute run-wait for users
    user_run_wait_map = {}
    with ThreadPoolExecutor(4) as thread_pool:
        result_iter = thread_pool.map(_handle_one_user, user_task_attempts_map.items())
    user_run_wait_map.update(result_iter)
    # print
    # print(user_run_wait_map)
    # pickle
    with open(dump_file, 'wb') as _f:
        pickle.dump(user_run_wait_map, _f)
    print('done')


# run
if __name__ == '__main__':
    main()
