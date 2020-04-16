import datetime
import copy


#=== classes ===================================================

# job chronicle point class
class ChroniclePoint(object):

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
        wait_duration = jobspec.startTime - jobspec.creationTime
        run_duration = jobspec.endTime - jobspec.startTime
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
            chronicle_point = ChroniclePoint(
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
