import os
import sys
import time
import datetime
import copy
import json
import pickle
import dbm
import csv
import threading
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore.core_utils import SQLiteProxy
from pandaatm.atmbody.agent_base import AgentBase
from pandaatm.atmutils.generic_utils import get_task_attempt_key_name, get_taskid_atmptn, update_set_by_change_tuple
from pandaatm.atmutils.slow_task_analyzer_utils import get_tasks_users_in_each_duration


# parameters
checkpoint_file_prefix = os.path.join('/tmp', 'user_run_wait_adv')
created_since = datetime.datetime(2020, 4, 10, 0, 0, 0)
created_before = datetime.datetime(2020, 4, 24, 0, 0, 0)
prod_source_label = 'user'
gshare = 'User Analysis'
running_slots_history_csv = '/tmp/user_run_wait_adv_running_slots_history.csv'


# internal constants
epoch = datetime.datetime(1970, 1, 1)
one_second = datetime.timedelta(seconds=1)


# global varibles
global_dict = {
        'agent': None,
        'jobspecs_db': None,
        '_running_slots_ts': np.array([]),
        '_running_slots_value': np.array([]),
        '_n_users_ts_period': np.array([]),
        '_n_users_value': np.array([]),
    }


# jobspecs db class
class JobspecsDB(object):

    def __init__(self, readonly=False):
        self.filename = global_dict['jobspecs_db']
        self.readonly = readonly
        if not self.readonly:
            self.initialize()
        self.open()

    # initialize jobspecs db (create tables etc)
    def initialize(self):
        create_table_sql = (
                'CREATE TABLE IF NOT EXISTS JobTable ('
                    'PandaID INTEGER NOT NULL PRIMARY KEY, '
                    'jediTaskID INTEGER NOT NULL, '
                    'attemptNr INTEGER NOT NULL, '
                    'userName TEXT, '
                    'jobStatus TEXT, '
                    'actualCoreCount INTEGER, '
                    'creationTime TIMESTAMP, '
                    'startTime TIMESTAMP, '
                    'endTime TIMESTAMP'
                    ')'
            )
        create_index_sql = (
                'CREATE INDEX idx_{column} '
                'ON JobTable({column}) '
            )
        index_column_list = ['jediTaskID', 'userName', 'creationTime']
        jdb = SQLiteProxy(self.filename)
        with jdb.get_write_proxy() as proxy:
            proxy.execute(create_table_sql)
            for column in index_column_list:
                proxy.execute(create_index_sql.format(column=column))
        jdb.close()

    # open jobspecs db
    def open(self):
        jdb = SQLiteProxy(self.filename, self.readonly)
        self.db = jdb

    # insert data into jobspecs db
    def insert(jobspec, userName, attemptNr):
        insert_sql = (
            'INSERT INTO JobTable ('
                'PandaID, jediTaskID, attemptNr, '
                'userName, jobStatus, actualCoreCount, '
                'creationTime, startTime, endTime) '
            'VALUES ('
            ':PandaID, :jediTaskID, :attemptNr, '
            ':userName, :jobStatus, :actualCoreCount, '
            ':creationTime, :startTime, :endTime) '
            )
        varMap = {
                ':PandaID': jobspec.PandaID,
                ':jediTaskID': jobspec.jediTaskID,
                ':attemptNr': attemptNr,
                ':userName': userName,
                ':jobStatus': jobspec.jobStatus,
                ':actualCoreCount': jobspec.actualCoreCount,
                ':creationTime': jobspec.creationTime,
                ':startTime': jobspec.startTime,
                ':endTime': jobspec.endTime,
            }
        self.db.cur.execute(insert_sql, varMap)

    # read data as a list of jobspecs
    def read_jobspecs(userName):
        read_sql = (
            'SELECT '
                'PandaID, jediTaskID, attemptNr, '
                'userName, jobStatus, actualCoreCount, '
                'creationTime, startTime, endTime '
            'FROM JobTable '
            'WHERE userName=:userName'
            )
        varMap = {
                ':userName': userName,
            }
        self.db.cur.execute(read_sql, varMap)
        ret_list = self.db.cur.fetchall()
        return ret_list

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
    ret = np.interp(real_ts_array, real_monitored_ts_array, global_dict['_running_slots_value'])
    return ret

# funciton to get number of users
def n_users_func(ts_array):
    ret_list = []
    first_start = global_dict['_n_users_ts_period'][0][0]
    first_value = global_dict['_n_users_value'][0]
    last_end = global_dict['_n_users_ts_period'][-1][1]
    last_value = global_dict['_n_users_value'][-1]
    # iterator
    vals_iter = zip(global_dict['_n_users_ts_period'], global_dict['_n_users_value'])
    # first values
    (_start, _end), _value = next(vals_iter)
    # loop over every second
    for ts in ts_array:
        # earlier than first record
        if ts < first_start:
            ret_list.append(first_value)
            continue
        # later than last record
        if ts >= last_end:
            ret_list.append(last_value)
            continue
        # within record
        if ts >= _start and ts < _end:
            ret_list.append(_value)
            continue
        # later than record, call next until within the record
        try:
            while ts >= _end:
                (_start, _end), _value = next(vals_iter)
        except StopIteration:
            # later than last record
            ret_list.append(last_value)
            continue
        else:
            # within the record
            ret_list.append(_value)
            continue

    # return
    ret = np.array(ret_list)
    return ret


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
        if global_dict['agent'] is None:
            global_dict['agent'] = AgentBase()
        agent = global_dict['agent']
        all_task_attempts_dict = agent.dbProxy.getTaskAttempts_ATM(
                                                created_since=created_since,
                                                created_before=created_before,
                                                prod_source_label=prod_source_label,
                                                gshare=gshare)
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
    # use sqlite to store jobspecs
    task_jobspecs_db_file = '{0}-task_jobspecs.db'.format(checkpoint_file_prefix)
    global_dict['jobspecs_db'] = task_jobspecs_db_file
    init_jobspecs_db()
    try:
        # open jobspecs db to read
        task_jobspecs_db_read = JobspecsDB(readonly=True)
    except sqlite3.OperationalError:
        # jobspecs db not existing
        # get db proxy
        if global_dict['agent'] is None:
            global_dict['agent'] = AgentBase()
        agent = global_dict['agent']
        # new jobspecs db to write
        task_jobspecs_db_write = JobspecsDB()
        # function to handle one task attempt
        def _handle_one_task_attempt(item):
            # start
            key, task_attempt = item
            jediTaskID, attemptNr = key
            key_name = get_task_attempt_key_name(jediTaskID, attemptNr)
            # get jobs
            with agent.dbProxyPool.get() as proxy:
                jobspec_list = proxy.slowTaskJobsInAttempt_ATM( jediTaskID=jediTaskID, attemptNr=attemptNr,
                                                                attempt_start=task_attempt.startTime,
                                                                attempt_end=task_attempt.endTime,
                                                                concise=True)
            # store into jobspecs db
            for jobspec in jobspec_list:
                task_jobspecs_db_write.insert(jobspec, task_attempt.userName, task_attempt.attemptNr)
        # parallel run with multithreading
        with ThreadPoolExecutor(4) as thread_pool:
            thread_pool.map(_handle_one_task_attempt, all_task_attempts_dict.items())
        # close jobspecs db
        task_jobspecs_db_write.db.close()
        del task_jobspecs_db_write
        # open jobspecs db to read
        task_jobspecs_db_read = JobspecsDB(readonly=True)
    print('got all jobspecs')
    # initialize functions of running_slots_history and n_users_history
    init_running_slots_history(running_slots_history_csv)
    print('initialized function by running slots history')
    init_n_users_history(period_list, n_users_in_duration_list)
    print('initialized function by n users history')
    # array of every second within the duration
    duration_ts_list = []
    tmp_ts = period_list[0][0]
    while tmp_ts < period_list[-1][1]:
        duration_ts_list.append(tmp_ts)
        tmp_ts += one_second
    duration_ts_array = np.array(duration_ts_list)
    print('got duration array')
    # array of multiplier (n_users / total_slots)
    multiplier_array = n_users_func(duration_ts_array) / running_slots_func(duration_ts_array)
    print('got multiplier array')
    # run time (weighted by cores_per_user) of jobs of the task attempt
    # initialize run wait dict for the user
    user_run_wait_map = {}
    all_users_tasks_dict = {}
    for key, task_attempt in all_task_attempts_dict.items():
        user_name = task_attempt.userName
        if user_name in all_users_tasks_dict:
            all_users_tasks_dict[user_name]['n_task_attempts'] += 1
            all_users_tasks_dict[user_name]['task_attempts'].add(key)
        else:
            all_users_tasks_dict[user_name] = {}
            all_users_tasks_dict[user_name]['n_task_attempts'] = 1
            all_users_tasks_dict[user_name]['task_attempts'] = set([key])
    for user_name, v in all_users_tasks_dict.items():
        user_run_wait_map[user_name] = {
                'total_jobs': 0,
                'total_task_attempts': v['n_task_attempts'],
                'total_taskful_time': datetime.timedelta(),
                'total_run_core_time': datetime.timedelta(),
                'total_successful_run_core_time': datetime.timedelta(),
                'total_run_time': datetime.timedelta(),
                'total_successful_run_time': datetime.timedelta(),
            }
    print('initialized user_run_wait_map')
    # aggregate taskful time in map
    tmp_user_set = set()
    for duration, user_change in zip(duration_list, user_name_change_in_duration_list):
        # update temporary sets
        update_set_by_change_tuple(tmp_user_set, user_change)
        # aggregate taskful time in map
        for user_name in tmp_user_set:
            user_run_wait_map[user_name]['total_taskful_time'] += duration
    print('computed taskful time for all users')



    # fill number and run core time of jobs of users
    for user_name in all_users_tasks_dict:
        the_jobs = task_jobspecs_db_read.read_jobspecs(userName=user_name)
        for PandaID, jediTaskID, attemptNr, \
                userName, jobStatus, actualCoreCount, \
                creationTime, startTime, endTime in the_jobs:
            if startTime in (None, 'NULL'):
                run_duration = datetime.timedelta()
            else:
                start_time = max(startTime, creationTime)
                run_duration = endTime - start_time
            run_core_time = run_duration*actualCoreCount if actualCoreCount not in (None, 'NULL') else datetime.timedelta()
            user_run_wait_map[user_name]['total_jobs'] += 1
            user_run_wait_map[user_name]['total_run_core_time'] += run_core_time
            if jobStatus == 'finished':
                user_run_wait_map[user_name]['total_successful_run_core_time'] += successful_run_core_time


    # run time (weighted by cores_per_user) of jobs of the task attempt
    n_periods = len(duration_list)
    tmp_key_set = set()
    tmp_user_set = set()
    nth_period = 0
    for period, duration, n_task_attempts, key_change, n_users, user_change in zip(*res_tasks_users):
        nth_period += 1
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
        n_users_array = n_users_func(duration_ts_array)
        running_slots_array = running_slots_func(duration_ts_array)
        # array of multipler (n_users / total_slots)
        multipler_array = n_users_array / running_slots_array
        # all task attempts in this duration
        # for key in tmp_key_set:
        def _handle_one_user_in_period(user_name):
            # fill jobspec list of the user
            the_jobs = task_jobspecs_db_read.read_jobspecs(userName=user_name)
            # numpy matrix for running period of all jobs
            jobs_matrix_list = []
            finished_jobs_matrix_list = []
            jobs_run_sec = 0
            finished_jobs_run_sec = 0
            for PandaID, jediTaskID, attemptNr, \
                    userName, jobStatus, actualCoreCount, \
                    creationTime, startTime, endTime in the_jobs:
                if startTime in (None, 'NULL'):
                    continue
                one_job_slots_list = []
                for ts in duration_ts_list:
                    if ts >= startTime and ts < endTime:
                        one_job_slots_list.append(actualCoreCount)
                    else:
                        one_job_slots_list.append(0)
                jobs_matrix_list.append(one_job_slots_list)
                if jobStatus == 'finished':
                    finished_jobs_matrix_list.append(one_job_slots_list)
            if jobs_matrix_list:
                jobs_matrix = np.array(jobs_matrix_list)
                # sum to get run time
                jobs_run_sec_array = np.dot(jobs_matrix, multipler_array)
                jobs_run_sec = np.sum(jobs_run_sec_array)
            if finished_jobs_matrix_list:
                finished_jobs_matrix = np.array(finished_jobs_matrix_list)
                # sum to get successful run time
                finished_jobs_run_sec_array = np.dot(finished_jobs_matrix, multipler_array)
                finished_jobs_run_sec = np.sum(finished_jobs_run_sec_array)
            # aggregate run time in map
            with global_lock:
                user_run_wait_map[user_name]['total_run_time'] += jobs_run_sec*one_second
                user_run_wait_map[user_name]['total_successful_run_time'] += finished_jobs_run_sec*one_second
        # parallel run with multithreading
        with ThreadPoolExecutor(8) as thread_pool:
            thread_pool.map(_handle_one_user_in_period, tmp_key_set)
        # aggregate taskful time in map
        for user_name in tmp_user_set:
            user_run_wait_map[user_name]['total_taskful_time'] += duration
        print('computed run time in a period: {0}/{1}'.format(nth_period, n_periods))
    print('computed run time for all users')


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
    print('obatained full run-wait information of all users')
    # close jobspecs db
    task_jobspecs_db_read.db.close()
    # print
    # print(user_run_wait_map)
    # pickle
    with open(dump_file, 'wb') as _f:
        pickle.dump(user_run_wait_map, _f)
    print('done')


# run
if __name__ == '__main__':
    main()
