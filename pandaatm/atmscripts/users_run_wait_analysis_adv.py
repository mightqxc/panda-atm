import os
import sys
import time
import datetime
import copy
import json
import pickle
import shelve
import dbm
import csv
import threading
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmbody.agent_base import AgentBase
from pandaatm.atmutils.generic_utils import get_task_attempt_name, get_taskid_atmptn, update_set_by_change_tuple
from pandaatm.atmutils.slow_task_analyzer_utils import get_total_jobs_run_core_time, get_tasks_users_in_each_duration


# parameters
checkpoint_file_prefix = os.path.join('/tmp', 'user_run_wait_adv')
created_since = datetime.datetime(2020, 4, 10, 0, 0, 0)
created_before = datetime.datetime(2020, 4, 24, 0, 0, 0)
prod_source_label = 'user'
gshare = 'User Analysis'
task_duration = datetime.timedelta(hours=1)
running_slots_history_csv = '/tmp/user_run_wait_adv_running_slots_history.csv'


# internal values
agent = None
epoch = datetime.datetime(1970, 1, 1)
one_second = datetime.timedelta(seconds=1)
# global varibles
global_dict = {
        '_running_slots_ts': np.array([]),
        '_running_slots_value': np.array([]),
        '_n_users_ts_period': np.array([]),
        '_n_users_value': np.array([]),
    }

# get history of running slots, csv from grafana plot
def init_running_slots_history(running_slots_history_csv):
    ts_list = []
    v_list = []
    with open(running_slots_history_csv, newline='') as csvfile:
        for row in csv.DictReader(csvfile, delimiter=';', quotechar='"'):
            if row['Series'] == gshare:
                timestamp = datetime.datetime.strptime(row['Time'], '%Y-%m-%dT%H:%M:%S+00:00')
                ts_list.append(timestamp)
                v_list.append(float(row['Value']))
    global_dict['_running_slots_ts'] = np.array(ts_list)
    global_dict['_running_slots_value'] = np.array(v_list, dtype='float64')

# set n_users by chronicle points
def init_n_users_history(period_list, n_users_in_duration_list):
    global_dict['_n_users_ts_period'] = np.array(period_list)
    global_dict['_n_users_value'] = np.array(n_users_in_duration_list)

# funciton to get running slots by interpolation
def running_slots_func(ts_array):
    real_ts_array = np.array([ x.total_seconds() for x in (ts_array - epoch) ])
    real_monitored_ts_array = np.array([ x.total_seconds() for x in (global_dict['_running_slots_ts'] - epoch) ])
    return np.interp(real_ts_array, real_monitored_ts_array, global_dict['_running_slots_value'])

# funciton to get number of users defined piecewise
def n_users_func(ts_array):
    condition_list = [ np.logical_and(ts_array >= _start, ts_array < _end) for _start, _end in global_dict['_n_users_ts_period'] ]
    return np.piecewise(ts_array, condition_list, global_dict['_n_users_value'])


# main
def main():
    dump_file = sys.argv[1]
    # start
    print('start')
    # global lock
    global_lock = threading.Lock()
    # checkpoint file for task attempts
    all_task_attempts_dict_file = '{0}-all_task_attempts_dict.pickle'.format(checkpoint_file_prefix)
    try:
        with open(all_task_attempts_dict_file, 'rb') as _f:
            all_task_attempts_dict = pickle.load(_f)
    except FileNotFoundError:
        # get db proxy
        if agent is None:
            agent = AgentBase()
        all_task_attempts_dict = agent.dbProxy.slowTaskAttempsFilter01_ATM(
                                            created_since=created_since,
                                            created_before=created_before,
                                            prod_source_label=prod_source_label,
                                            gshare=gshare,
                                            task_duration=task_duration)
        # pickle for checkpoint
        with open(all_task_attempts_dict_file, 'wb') as _f:
            pickle.dump(all_task_attempts_dict, _f)
    print('got all task attempts')
    # handle all task attempts
    res_tasks_users = get_tasks_users_in_each_duration(all_task_attempts_dict)
    (   period_list, duration_list,
        n_tasks_in_duration_list, task_attempt_change_in_duration_list,
        n_users_in_duration_list, user_name_change_in_duration_list) = res_tasks_users
    print('handed all task attempts')
    # use shelve to store jobspecs
    task_jobspecs_shelve_file = '{0}-task_jobspecs.shelve'.format(checkpoint_file_prefix)
    try:
        # open shelve to read
        task_jobspecs_shelve = shelve.open(task_jobspecs_shelve_file, flag='r')
    except dbm.error:
        # get db proxy
        if agent is None:
            agent = AgentBase()
        # new shelve to write
        task_jobspecs_shelve = shelve.open(task_jobspecs_shelve_file, flag='c', writeback=False)
        # function to handle one task
        def _handle_one_task(item):
            # start
            k, v = item
            jediTaskID, attemptNr = k
            task_attempt_name = get_task_attempt_name(jediTaskID, attemptNr)
            # get jobs
            with agent.dbProxyPool.get() as proxy:
                jobspec_list = proxy.slowTaskJobsInAttempt_ATM( jediTaskID=jediTaskID, attemptNr=attemptNr,
                                                                attempt_start=v['startTime'], attempt_end=v['endTime'],
                                                                concise=True)
            # store into shelve
            with global_lock:
                task_jobspecs_shelve[task_attempt_name] = jobspec_list
        # parallel run with multithreading
        with ThreadPoolExecutor(4) as thread_pool:
            thread_pool.map(_handle_one_task, all_task_attempts_dict.items())
        # close shelve
        task_jobspecs_shelve.close()
        del task_jobspecs_shelve
        # open shelve to read
        task_jobspecs_shelve = shelve.open(task_jobspecs_shelve_file, flag='r')
    print('got all jobspecs')
    # initialize functions of running_slots_history and n_users_history
    init_running_slots_history(running_slots_history_csv)
    print('initialized function by running slots history')
    init_n_users_history(period_list, n_users_in_duration_list)
    print('initialized function by n users history')
    # run time (weighted by cores_per_user) of jobs of the task attempt
    # initialize run wait dict for the user
    user_run_wait_map = {}
    all_users_tasks_dict = {}
    for v in all_task_attempts_dict.values():
        user_name = v['userName']
        if user_name in all_users_tasks_dict:
            all_users_tasks_dict[user_name] += 1
        else:
            all_users_tasks_dict[user_name] = 1
    for user_name, n_task_attempts in all_users_tasks_dict.items():
        user_run_wait_map[user_name] = {
                'total_jobs': 0,
                'total_task_attempts': n_task_attempts,
                'total_taskful_time': datetime.timedelta(),
                'total_run_time': datetime.timedelta(),
                'total_successful_run_time': datetime.timedelta(),
            }
    # run time (weighted by cores_per_user) of jobs of the task attempt
    tmp_key_set = set()
    tmp_user_set = set()
    for period, duration, n_task_attempts, key_change, n_users, user_change in zip(*res_tasks_users):
        # skip if no task attempt
        if n_task_attempts == 0:
            continue
        # update temporary sets
        update_set_by_change_tuple(tmp_key_set, key_change)
        update_set_by_change_tuple(tmp_user_set, user_change)
        # array of every second within the duration
        duration_ts_list = []
        tmp_ts = period[0]
        while tmp_ts < period[1]:
            duration_ts_list.append(tmp_ts)
            tmp_ts += one_second
        duration_ts_array = np.array(duration_ts_list)
        # array of muliplier (n_users / total_slots)
        muliplier_array = n_users_func(duration_ts_array) / running_slots_func(duration_ts_array)
        # all task attempts in this duration
        for key in tmp_key_set:
            task_attempt_name = get_task_attempt_name(*key)
            # user of this task attempt
            user_name = all_task_attempts_dict[key]['userName']
            # all jobs in this task attempt
            jobspec_list = task_jobspecs_shelve[task_attempt_name]
            # numpy matrix for running period of all jobs
            jobs_matrix_list = []
            finished_jobs_matrix_list = []
            jobs_run_sec = 0
            finished_jobs_run_sec = 0
            for jobspec in jobspec_list:
                user_run_wait_map[user_name]['total_jobs'] += 1
                if jobspec.startTime in (None, 'NULL'):
                    continue
                one_job_slots_list = []
                for ts in duration_ts_list:
                    if ts >= jobspec.startTime and ts < jobspec.endTime:
                        one_job_slots_list.append(jobspec.actualCoreCount)
                    else:
                        one_job_slots_list.append(0)
                jobs_matrix_list.append(one_job_slots_list)
                if jobspec.jobStatus == 'finished':
                    finished_jobs_matrix_list.append(one_job_slots_list)
            if jobs_matrix_list:
                jobs_matrix = np.array(jobs_matrix_list)
                # sum to get run time
                jobs_run_sec_array = np.dot(jobs_matrix, muliplier_array)
                jobs_run_sec = np.sum(jobs_run_sec_array)
            if finished_jobs_matrix_list:
                finished_jobs_matrix = np.array(finished_jobs_matrix_list)
                # sum to get successful run time
                finished_jobs_run_sec_array = np.dot(finished_jobs_matrix, muliplier_array)
                finished_jobs_run_sec = np.sum(finished_jobs_run_sec_array)
            # aggregate run time in map
            user_run_wait_map[user_name]['total_run_time'] += jobs_run_sec*one_second
            user_run_wait_map[user_name]['total_successful_run_time'] += finished_jobs_run_sec*one_second
        # aggregate taskful time in map
        for user_name in tmp_user_set:
            user_run_wait_map[user_name]['total_taskful_time'] += duration
    # compute remaining values
    for user_name in all_users_tasks_dict:
        v = user_run_wait_map[user_name]
        total_wait_time = v['total_taskful_time'] - v['total_run_time']
        total_run_proportion = v['total_run_time']/v['total_taskful_time']
        total_successful_run_proportion = v['total_successful_run_time']/v['total_taskful_time']
        total_wait_proportion = total_wait_time/v['total_taskful_time']
        v.update({
            'total_wait_time': total_wait_time,
            'total_run_proportion': total_run_proportion,
            'total_successful_run_proportion': total_successful_run_proportion,
            'total_wait_proportion': total_wait_proportion,
            })
    print('obatained run-wait information of all users')
    # close shelve
    task_jobspecs_shelve.close()
    # print
    # print(user_run_wait_map)
    # pickle
    with open(dump_file, 'wb') as _f:
        pickle.dump(user_run_wait_map, _f)
    print('done')


# run
if __name__ == '__main__':
    main()
