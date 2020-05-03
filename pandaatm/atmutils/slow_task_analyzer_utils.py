import datetime
import copy

from pandaatm.atmutils.generic_utils import get_change_of_set


#=== classes ===================================================

# job chronicle point class
class JobChroniclePoint(object):

    __slots__ = [
            'timestamp',
            'type',
            'PandaID',
            'jobStatus',
        ]

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __eq__(self, other):
        return self.timestamp == other.timestamp

    def __str__(self):
        ret = f'(ts={self.timestamp}, pandaid={self.PandaID}, type={self.type}, status={self.jobStatus})'
        return ret

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# task chronicle point class
class TaskChroniclePoint(object):

    __slots__ = [
            'timestamp',
            'type',
            'jediTaskID',
            'attemptNr',
            'status',
        ]

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __eq__(self, other):
        return self.timestamp == other.timestamp

    def __str__(self):
        ret = f'(ts={self.timestamp}, taskid={self.jediTaskID}, atmpt={self.attemptNr}, type={self.type}, status={self.status})'
        return ret

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


#=== methods ===================================================

def get_job_durations(jobspec):
    """
    get wait_duration and run_duration of a jobspec
    :return: (wait_duration, run_duration)
    :rtype: tuple
    """
    if jobspec.startTime in (None, 'NULL'):
        wait_duration = jobspec.endTime - jobspec.creationTime
        run_duration = datetime.timedelta()
    else:
        # in case some strange jobs with startTime < creationTime
        start_time = max(jobspec.startTime, jobspec.creationTime)
        wait_duration = start_time - jobspec.creationTime
        run_duration = jobspec.endTime - start_time
    return wait_duration, run_duration

def get_jobs_time_consumption_statistics(jobspec_list):
    """
    get statistics of time consumption of jobs, classified by different types
    """
    # get chronicle points of the jobs and sort in time order
    chronicle_point_list = []
    for jobspec in jobspec_list:
        for attr in ['creationTime', 'startTime', 'endTime']:
            timestamp = getattr(jobspec, attr)
            if timestamp in (None, 'NULL'):
                continue
            chronicle_point = JobChroniclePoint(
                                    timestamp=timestamp,
                                    type=attr,
                                    PandaID=jobspec.PandaID,
                                    jobStatus=jobspec.jobStatus,
                                )
            chronicle_point_list.append(chronicle_point)
    chronicle_point_list.sort()
    # initialization
    duration_list = []
    n_jobs_in_durations_dict = {
            'finished': {
                'run': [],
                'wait': [],
            },
            'failed': {
                'run': [],
                'wait': [],
            },
            'closed': {
                'run': [],
                'wait': [],
            },
            'cancelled': {
                'run': [],
                'wait': [],
            },
        }
    time_consumption_stats_dict = {
            'finished': {
                'run': datetime.timedelta(),
                'wait': datetime.timedelta(),
                'total': datetime.timedelta(),
            },
            'failed': {
                'run': datetime.timedelta(),
                'wait': datetime.timedelta(),
                'total': datetime.timedelta(),
            },
            'closed': {
                'run': datetime.timedelta(),
                'wait': datetime.timedelta(),
                'total': datetime.timedelta(),
            },
            'cancelled': {
                'run': datetime.timedelta(),
                'wait': datetime.timedelta(),
                'total': datetime.timedelta(),
            },
            'total': {
                'run': datetime.timedelta(),
                'wait': datetime.timedelta(),
                'total': datetime.timedelta(),
            },
        }
    jobs_record_dict = {
            'finished': {
                'run': set(),
                'wait': set(),
            },
            'failed': {
                'run': set(),
                'wait': set(),
            },
            'closed': {
                'run': set(),
                'wait': set(),
            },
            'cancelled': {
                'run': set(),
                'wait': set(),
            },
        }
    previous_point = None
    # loop over chronicle points
    for chronicle_point in chronicle_point_list:
        if previous_point is not None:
            # duration
            duration = chronicle_point.timestamp - previous_point.timestamp
            # record duration
            duration_list.append(duration)
            # from previous point
            status = previous_point.jobStatus
            pandaid = previous_point.PandaID
            cptype = previous_point.type
            if cptype == 'creationTime':
                # job waiting in this duration
                jobs_record_dict[status]['wait'].add(pandaid)
            elif cptype == 'startTime':
                # job running in this duration
                jobs_record_dict[status]['wait'].discard(pandaid)
                jobs_record_dict[status]['run'].add(pandaid)
            elif cptype == 'endTime':
                # job ended, clear from temp records
                jobs_record_dict[status]['wait'].discard(pandaid)
                jobs_record_dict[status]['run'].discard(pandaid)
            else:
                # should not happen
                raise RuntimeError('Bad type of chronicle point: {0}'.format(cptype))
            # compute number of jobs in this duration
            n_total_jobs = 0
            for _status in ['finished', 'failed', 'closed', 'cancelled']:
                for _dur_type in ['wait', 'run']:
                    n_total_jobs += len(jobs_record_dict[_status][_dur_type])
            # aggregate time consumed in stats and record n_jobs
            for _status in ['finished', 'failed', 'closed', 'cancelled']:
                for _dur_type in ['wait', 'run']:
                    n_jobs = len(jobs_record_dict[_status][_dur_type])
                    n_jobs_in_durations_dict[_status][_dur_type].append(n_jobs)
                    if n_total_jobs > 0:
                        ratio = n_jobs / n_total_jobs
                        time_consumed = duration * ratio
                        time_consumption_stats_dict[_status][_dur_type] += time_consumed
                        time_consumption_stats_dict['total'][_dur_type] += time_consumed
                        time_consumption_stats_dict[_status]['total'] += time_consumed
                        time_consumption_stats_dict['total']['total'] += time_consumed
        # prepare for next loop
        previous_point = chronicle_point
    # handle after last loop
    if previous_point is not None:
        _status = previous_point.jobStatus
        _pandaid = previous_point.PandaID
        _cptype = previous_point.type
        if _cptype == 'creationTime':
            # job waiting in this duration
            jobs_record_dict[_status]['wait'].add(_pandaid)
        elif _cptype == 'startTime':
            # job running in this duration
            jobs_record_dict[_status]['wait'].discard(_pandaid)
            jobs_record_dict[_status]['run'].add(_pandaid)
        elif _cptype == 'endTime':
            # job ended, clear from temp records
            jobs_record_dict[_status]['wait'].discard(_pandaid)
            jobs_record_dict[_status]['run'].discard(_pandaid)
    # check
    n_durations = len(duration_list)
    for _status in ['finished', 'failed', 'closed', 'cancelled']:
        for _dur_type in ['wait', 'run']:
            # with jobs left over; should not happen
            if len(jobs_record_dict[_status][_dur_type]) > 0:
                raise RuntimeError('still some jobs left over: {0}'.format(jobs_record_dict))
            # differnt number of job-duration records; should not happen
            n_jobs_records = len(n_jobs_in_durations_dict[_status][_dur_type])
            if n_jobs_records != n_durations:
                raise RuntimeError('numbers of job-duration records do not match: {0}_{1} has {2} != {3}'.format(
                                                                    _status, _dur_type, n_jobs_records, n_durations))
    # summary
    summary_dict = copy.deepcopy(time_consumption_stats_dict)
    # return
    return summary_dict

def get_total_jobs_run_core_time(jobspec_list):
    """
    get sum of run core time (~ cputime) and only successful one of all jobs
    """
    run_core_time = datetime.timedelta()
    successful_run_core_time = datetime.timedelta()
    for jobspec in jobspec_list:
        wait_duration, run_duration = get_job_durations(jobspec)
        core_time = run_duration*jobspec.actualCoreCount
        run_core_time += core_time
        if jobspec.jobStatus == 'finished':
            successful_run_core_time += core_time
    return run_core_time, successful_run_core_time

def get_task_attempts_in_each_duration(task_attempt_dict):
    """
    get a tuple of lists task attempts in each (non-overlap) duration
    """
    # get chronicle points of the jobs and sort in time order
    chronicle_point_list = []
    for k, v in task_attempt_dict.items():
        jediTaskID, attemptNr = k
        for attr in ['startTime', 'endTime']:
            timestamp = v[attr]
            if timestamp in (None, 'NULL'):
                continue
            chronicle_point = TaskChroniclePoint(
                                    timestamp=timestamp,
                                    type=attr,
                                    jediTaskID=jediTaskID,
                                    attemptNr=attemptNr,
                                    status=v['finalStatus'],
                                )
            chronicle_point_list.append(chronicle_point)
    chronicle_point_list.sort()
    # initialization
    duration_list = []
    n_tasks_in_duration_list = []
    task_attempts_in_duration_list = []
    task_record_set = set()
    previous_point = None
    # loop over chronicle points
    for chronicle_point in chronicle_point_list:
        if previous_point is not None:
            # duration
            duration = chronicle_point.timestamp - previous_point.timestamp
            # record duration
            duration_list.append(duration)
            # from previous point
            key = (previous_point.jediTaskID, previous_point.attemptNr)
            cptype = previous_point.type
            if cptype == 'startTime':
                # task attempt in this duration
                task_record_set.add(key)
            elif cptype == 'endTime':
                # task attempt ended, clear from temp records
                task_record_set.discard(key)
            else:
                # should not happen
                raise RuntimeError('Bad type of chronicle point: {0}'.format(cptype))
            # compute number of task attempts in this duration
            n_total_task_attempts = len(task_record_set)
            n_tasks_in_duration_list.append(n_total_task_attempts)
            # record all task attemps in this duration
            task_attempts_in_duration_list.append(set(task_record_set))
        # prepare for next loop
        previous_point = chronicle_point
    # handle after last loop
    if previous_point is not None:
        _key = (previous_point.jediTaskID, previous_point.attemptNr)
        _cptype = previous_point.type
        if _cptype == 'startTime':
            # task attempt in this duration
            task_record_set.add(_key)
        elif _cptype == 'endTime':
            # task attempt ended, clear from temp records
            task_record_set.discard(_key)
    # check
    n_durations = len(duration_list)
    # with task attempt left over; should not happen
    if len(task_record_set) > 0:
        raise RuntimeError('still some task attempts left over: {0}'.format(task_record_set))
    # differnt number of records; should not happen
    n_tasks_records = len(n_tasks_in_duration_list)
    if n_tasks_records != n_durations:
        raise RuntimeError('numbers of task attempt duration records do not match: {0} != {1}'.format(
                                                                                    n_tasks_records, n_durations))
    # return
    return duration_list, n_tasks_in_duration_list, task_attempts_in_duration_list

def get_tasks_users_in_each_duration(all_task_attempts_dict):
    """
    get a tuple of lists task attempts and user in each (non-overlap) duration
    """
    # get chronicle points of the jobs and sort in time order
    chronicle_point_list = []
    for k, v in all_task_attempts_dict.items():
        jediTaskID, attemptNr = k
        for attr in ['startTime', 'endTime']:
            timestamp = v[attr]
            if timestamp in (None, 'NULL'):
                continue
            chronicle_point = TaskChroniclePoint(
                                    timestamp=timestamp,
                                    type=attr,
                                    jediTaskID=jediTaskID,
                                    attemptNr=attemptNr,
                                    status=v['finalStatus'],
                                )
            chronicle_point_list.append(chronicle_point)
    chronicle_point_list.sort()
    # period list
    period_list = []
    if chronicle_point_list:
        for j in range(len(chronicle_point_list) - 1):
            period_list.append((chronicle_point_list[j].timestamp,
                                chronicle_point_list[j+1].timestamp))
    # initialization
    duration_list = []
    n_tasks_in_duration_list = []
    n_users_in_duration_list = []
    task_attempt_change_in_duration_list = []
    user_name_change_in_duration_list = []
    task_record_set = set()
    user_record_set = set()
    previous_point = None
    # loop over chronicle points
    for chronicle_point in chronicle_point_list:
        if previous_point is not None:
            # duration
            duration = chronicle_point.timestamp - previous_point.timestamp
            # record duration
            duration_list.append(duration)
            # from previous point
            key = (previous_point.jediTaskID, previous_point.attemptNr)
            cptype = previous_point.type
            user_name = all_task_attempts_dict[key]['userName']
            if cptype == 'startTime':
                # record the increment by this task attemp and user in this duration
                task_attempt_change_in_duration_list.append(get_change_of_set(task_record_set, key, '+'))
                user_name_change_in_duration_list.append(get_change_of_set(user_record_set, user_name, '+'))
                # task attempt in this duration
                task_record_set.add(key)
                user_record_set.add(user_name)
            elif cptype == 'endTime':
                # record the increment by this task attemp and user in this duration
                task_attempt_change_in_duration_list.append(get_change_of_set(task_record_set, key, '-'))
                user_name_change_in_duration_list.append(get_change_of_set(user_record_set, user_name, '-'))
                # task attempt ended, clear from temp records
                task_record_set.discard(key)
                user_record_set.discard(user_name)
            else:
                # should not happen
                raise RuntimeError('Bad type of chronicle point: {0}'.format(cptype))
            # number of task attempts and users in this duration
            n_total_task_attempts = len(task_record_set)
            n_total_users = len(user_record_set)
            n_tasks_in_duration_list.append(n_total_task_attempts)
            n_users_in_duration_list.append(n_total_users)
        # prepare for next loop
        previous_point = chronicle_point
    # handle after last loop
    if previous_point is not None:
        _key = (previous_point.jediTaskID, previous_point.attemptNr)
        _cptype = previous_point.type
        if _cptype == 'startTime':
            # task attempt in this duration
            task_record_set.add(_key)
        elif _cptype == 'endTime':
            # task attempt ended, clear from temp records
            task_record_set.discard(_key)
    # check
    n_durations = len(duration_list)
    # with task attempt left over; should not happen
    if len(task_record_set) > 0:
        raise RuntimeError('still some task attempts left over: {0}'.format(task_record_set))
    # differnt number of records; should not happen
    n_tasks_records = len(n_tasks_in_duration_list)
    if n_tasks_records != n_durations:
        raise RuntimeError('numbers of task attempt duration records do not match: {0} != {1}'.format(
                                                                                    n_tasks_records, n_durations))
    # return
    return (period_list, duration_list,
            n_tasks_in_duration_list, task_attempt_change_in_duration_list,
            n_users_in_duration_list, user_name_change_in_duration_list)


#=== test functions of bad jobs ===============================

def bad_job_test_main(jobspec_list):
    """
    main to loop over other test functions
    """
    bad_job_test_functions = [
            test_priority_too_low,
        ]
    retList = []
    for func in bad_job_test_functions:
        retList.append(func(jobspec_list))
    return retList

def test_priority_too_low(jobspec_list):
    """
    test if priority of jobs too low
    """
    # symptom tag
    symptom_tag = 'JobsPriorityTooLow'
    # return value and message
    retVal = False
    retMsg = ''
    # criteria
    prio_threshold = -500
    ratio_low_prio_threshold = 0.3
    n_jobs = len(jobspec_list)
    n_jobs_low_prio = 0
    sum_low_prio = 0
    for jobspec in jobspec_list:
        if jobspec.currentPriority < prio_threshold:
            n_jobs_low_prio += 1
            sum_low_prio += jobspec.currentPriority
    if n_jobs == 0:
        ratio_low_prio = 0
        avg_low_prio = 0
    else:
        ratio_low_prio = n_jobs_low_prio / n_jobs
        avg_low_prio = sum_low_prio / n_jobs_low_prio if n_jobs_low_prio > 0 else 0
    if ratio_low_prio > ratio_low_prio_threshold:
        retVal = True
        retMsg = '{0} jobs ({1}%) have low priority (avg {2})'.format(
                        n_jobs_low_prio,
                        int(ratio_low_prio*100),
                        int(avg_low_prio),
                    )
    # return
    return retVal, symptom_tag, retMsg
