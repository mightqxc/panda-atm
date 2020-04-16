import os
import time
import datetime
import copy
import json

from pandacommon.pandalogger import logger_utils

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmbody.agent_base import AgentBase
from pandaatm.atmutils.slow_task_analyzer_utils import get_job_durations, get_jobs_time_consumption_statistics, bad_job_test_main


base_logger = logger_utils.setup_logger('slow_task_analyzer')


# agent class
class SlowTaskAnalyzer(AgentBase):

    def __init__(self):
        super().__init__()
        # parameters
        self.sleepPeriod = 300
        self.sinceHours = 336
        self.taskDurationMaxHours = 168
        self.taskSuccefulRunTimeMinPercent = 80
        self.taskEachStatusMaxHours = 12
        self.joblessIntervalMaxHours = 16
        self.jobBadTimeMaxPercent = 10
        self.jobMaxHoursMap = {
                'finished': {'wait': 16, 'run': 96},
                'failed': {'wait': 16, 'run': 16},
                'cancelled': {'wait': 16, 'run': 16},
                'closed': {'wait': 12, 'run': 16},
            }
        self.reportDir = '/tmp/slow_task_dumps'

    def _slow_task_attempts_display(self, ret_dict: dict) -> str :
        result_str_line_template = '{jediTaskID:>10}  {attemptNr:>4} | {finalStatus:>10} {startTime:>20}  {endTime:>20}  {attemptDuration:>15}    {successful_run_time_ratio:>6} '
        result_str_list = []
        result_str_list.append(result_str_line_template.format(jediTaskID='taskID', attemptNr='#N', finalStatus='status', startTime='startTime', endTime='endTime', attemptDuration='duration', successful_run_time_ratio='SRTR%'))
        for k in sorted(ret_dict):
            v = ret_dict[k]
            result_str_line = result_str_line_template.format(
                                                                jediTaskID=k[0],
                                                                attemptNr=k[1],
                                                                finalStatus=v['finalStatus'],
                                                                startTime=v['startTime'].strftime('%y-%m-%d %H:%M:%S'),
                                                                endTime=v['endTime'].strftime('%y-%m-%d %H:%M:%S'),
                                                                attemptDuration=core_utils.timedelta_parse_dict(v['attemptDuration'])['str_dcolon'],
                                                                successful_run_time_ratio='{0:.2f}%'.format(v['jobs_time_consumption_stats_dict']['_successful_run_time_ratio']*100),
                                                                )
            result_str_list.append(result_str_line)
        return '\n'.join(result_str_list)

    def _get_job_attr_dict(self, jobspec):
        wait_duration, run_duration = get_job_durations(jobspec)
        diag_display_str_list = []
        if jobspec.transExitCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('trans-{0}'.format(jobspec.transExitCode))
        if jobspec.pilotErrorCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('pilot-{0}: {1}'.format(jobspec.pilotErrorCode, jobspec.pilotErrorDiag))
        if jobspec.exeErrorCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('exe-{0}: {1}'.format(jobspec.exeErrorCode, jobspec.exeErrorDiag))
        if jobspec.ddmErrorCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('ddm-{0}: {1}'.format(jobspec.ddmErrorCode, jobspec.ddmErrorDiag))
        if jobspec.brokerageErrorCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('brokr-{0}: {1}'.format(jobspec.brokerageErrorCode, jobspec.brokerageErrorDiag))
        if jobspec.jobDispatcherErrorCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('jdisp-{0}: {1}'.format(jobspec.jobDispatcherErrorCode, jobspec.jobDispatcherErrorDiag))
        if jobspec.taskBufferErrorCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('tbuf-{0}: {1}'.format(jobspec.taskBufferErrorCode, jobspec.taskBufferErrorDiag))
        if jobspec.supErrorCode not in (None, 0, 'NULL', '0'):
            diag_display_str_list.append('sup-{0}: {1}'.format(jobspec.supErrorCode, jobspec.supErrorDiag))
        retDict = {
                'PandaID': jobspec.PandaID,
                'jobStatus': jobspec.jobStatus,
                'priority': jobspec.currentPriority,
                'waitDuration': wait_duration,
                'runDuration': run_duration,
                'errorInfo': '{0:>11} | {1:>24} | '.format(jobspec.jobStatus, jobspec.computingSite) + '; '.join(diag_display_str_list),
            }
        return retDict

    def _get_task_status_log(self, status_list):
        status_log_list = []
        last_modification_time = None
        for i in range(len(status_list)):
            status, modificationTime = status_list[i]
            if i >= 1:
                last_duration = modificationTime - last_modification_time
                status_log_list[i-1]['duration'] = last_duration
            status_log_dict = {
                    'status': status,
                    'modificationTime': modificationTime,
                    'duration': datetime.timedelta(),
                }
            status_log_list.append(status_log_dict)
            last_modification_time = modificationTime
        return status_log_list

    def _search_long_status(self, status_log_list):
        long_status_log_list = []
        for status_log_dict in status_log_list:
            if status_log_dict['status'] not in ('scouting', 'running', 'processing') \
                and status_log_dict['duration'] > datetime.timedelta(hours=self.taskEachStatusMaxHours):
                long_status_log_list.append(status_log_dict)
        return long_status_log_list

    def _long_status_display(self, long_status_log_list) -> str:
        result_str_line_template = '  {status:>11} | {modificationTime:>17} | {duration:>15}'
        result_str_list = []
        result_str_list.append(result_str_line_template.format(
                status='status',
                modificationTime='modificationTime',
                duration='duration',
            ))
        for status_log_dict in long_status_log_list:
            result_str_list.append(result_str_line_template.format(
                    status=status_log_dict['status'],
                    modificationTime=status_log_dict['modificationTime'].strftime('%y-%m-%d %H:%M:%S'),
                    duration=core_utils.timedelta_parse_dict(status_log_dict['duration'])['str_dcolon'],
                ))
        result_str = '\n'.join(result_str_list)
        return result_str

    def _search_bad_intervals(self, jobspec_list, attempt_start):
        bad_interval_list = []
        last_jobful_time = attempt_start
        last_job_pandaid = None
        for jobspec in sorted(jobspec_list, key=(lambda x: x.creationTime)):
            if jobspec.endTime <= last_jobful_time:
                continue
            if jobspec.creationTime <= last_jobful_time:
                # jobs duration overlap, good
                pass
            else:
                # jobless interval
                interval = jobspec.creationTime - last_jobful_time
                if interval > datetime.timedelta(hours=self.joblessIntervalMaxHours):
                    bad_interval_dict = {
                            'duration': interval,
                            'lastJobPandaID': last_job_pandaid,
                            'lastJobEndTime': last_jobful_time,
                            'nextJobPandaID': jobspec.PandaID,
                            'nextJobCreationTime': jobspec.creationTime,
                        }
                    bad_interval_list.append(bad_interval_dict)
            last_jobful_time = jobspec.endTime
            last_job_pandaid = jobspec.PandaID
        return bad_interval_list

    def _bad_intervals_display(self, bad_interval_list) -> str:
        result_str_line_template = '  {lastJobPandaID:>20} , {lastJobEndTime_str:>17} | {nextJobPandaID:>20} , {nextJobCreationTime_str:>17} |  {duration_str:>15}'
        result_str_list = []
        result_str_list.append(result_str_line_template.format(
                lastJobPandaID='PreviousJob PanDAID',
                lastJobEndTime_str='Ended At',
                nextJobPandaID='FollowingJob PanDAID',
                nextJobCreationTime_str='Created At',
                duration_str='Duration',
            ))
        for gap in bad_interval_list:
            result_str_list.append(result_str_line_template.format(
                    lastJobPandaID=gap['lastJobPandaID'],
                    lastJobEndTime_str=gap['lastJobEndTime'].strftime('%y-%m-%d %H:%M:%S'),
                    nextJobPandaID=gap['nextJobPandaID'],
                    nextJobCreationTime_str=gap['nextJobCreationTime'].strftime('%y-%m-%d %H:%M:%S'),
                    duration_str=core_utils.timedelta_parse_dict(gap['duration'])['str_dcolon'],
                ))
        result_str = '\n'.join(result_str_list)
        return result_str

    def _bad_job_time_consumed_set(self, real_task_duration, jobs_time_consumption_stats_dict):
        ret_msg_set = set()
        for status in ['finished', 'failed', 'closed', 'cancelled']:
            for dur_type in ['wait', 'run']:
                if (status, dur_type) == ('finished', 'run'):
                    continue
                if jobs_time_consumption_stats_dict[status][dur_type]*100/real_task_duration >= self.jobBadTimeMaxPercent:
                    msg_tag = 'Job{0}{1}Long'.format(status.capitalize(), dur_type.capitalize())
                    ret_msg_set.add(msg_tag)
        return ret_msg_set

    def _bad_job_qualify(self, job_attr_dict):
        retVal = False
        status = job_attr_dict['jobStatus']
        # according to status
        for status in ['finished', 'failed', 'closed', 'cancelled']:
            if status != job_attr_dict['jobStatus']:
                continue
            for dur_type in ['wait', 'run']:
                dur_name = '{0}Duration'.format(dur_type)
                if job_attr_dict[dur_name] > datetime.timedelta(hours=self.jobMaxHoursMap[status][dur_type]):
                    retVal = True
        return retVal

    def _bad_jobs_display(self, pandaid_list, err_info_dict) -> str:
        sorted_err_info_list = sorted(err_info_dict.items(), key=(lambda x: (x[1]['n_jobs'], x[1]['waitDuration'] + x[1]['runDuration'])), reverse=True)
        errors_str = '\n    '.join([ '{n_jobs:>6} | {avg_wait:>12} | {avg_run:>12} | {avg_prio:>7} | {info}'.format(
                                            n_jobs=x[1]['n_jobs'],
                                            avg_wait=core_utils.timedelta_parse_dict(x[1]['waitDuration']/x[1]['n_jobs'])['str_dcolon'],
                                            avg_run=core_utils.timedelta_parse_dict(x[1]['runDuration']/x[1]['n_jobs'])['str_dcolon'],
                                            avg_prio=int(x[1]['priority']/x[1]['n_jobs']),
                                            info=x[0]
                                        ) for x in sorted_err_info_list ])
        result_str = (
                '  Jobs: {pandaids_str}\n'
                '     NJobs |   AvgWaiting |   AvgRunning | AvgPrio |   jobStatus |            computingSite | Dialogs \n'
                '    {errors_str}\n'
            ).format(
                    pandaids_str=','.join(sorted([ str(i) for i in pandaid_list ])),
                    errors_str=errors_str
                )
        return result_str

    def _jobs_time_consumption_stats_display(self, real_task_duration, jobs_time_consumption_stats_dict) -> str:
        # function to format one data record
        def get_data_str(data_time):
            data_str = '{duration:>13} ({percent:>2}%)'.format(
                    duration=core_utils.timedelta_parse_dict(data_time)['str_dcolon'],
                    percent=int(data_time*100/real_task_duration),
                )
            return data_str
        # dict result dict
        result_dict = {
                'wfn': get_data_str(jobs_time_consumption_stats_dict['finished']['wait']),
                'wfa': get_data_str(jobs_time_consumption_stats_dict['failed']['wait']),
                'wcl': get_data_str(jobs_time_consumption_stats_dict['closed']['wait']),
                'wcn': get_data_str(jobs_time_consumption_stats_dict['cancelled']['wait']),
                'wto': get_data_str(jobs_time_consumption_stats_dict['total']['wait']),
                'rfn': get_data_str(jobs_time_consumption_stats_dict['finished']['run']),
                'rfa': get_data_str(jobs_time_consumption_stats_dict['failed']['run']),
                'rcl': get_data_str(jobs_time_consumption_stats_dict['closed']['run']),
                'rcn': get_data_str(jobs_time_consumption_stats_dict['cancelled']['run']),
                'rto': get_data_str(jobs_time_consumption_stats_dict['total']['run']),
                'tfn': get_data_str(jobs_time_consumption_stats_dict['finished']['total']),
                'tfa': get_data_str(jobs_time_consumption_stats_dict['failed']['total']),
                'tcl': get_data_str(jobs_time_consumption_stats_dict['closed']['total']),
                'tcn': get_data_str(jobs_time_consumption_stats_dict['cancelled']['total']),
                'tto': get_data_str(jobs_time_consumption_stats_dict['total']['total']),
            }
        # get result string
        result_str = (
                '             |              waiting |              running |                total | \n'
                '    finished | {wfn:>20} | {rfn:>20} | {tfn:>20} | \n'
                '      failed | {wfa:>20} | {rfa:>20} | {tfa:>20} | \n'
                '      closed | {wcl:>20} | {rcl:>20} | {tcl:>20} | \n'
                '   cancelled | {wcn:>20} | {rcn:>20} | {tcn:>20} | \n'
                '       total | {wto:>20} | {rto:>20} | {tto:>20} | \n'
                '\n'
                '  {summary}\n'
            ).format(
                    summary='jobful time: {0:.2f}% , successful run time: {1:.2f}%'.format(
                                jobs_time_consumption_stats_dict['_jobful_time_ratio']*100,
                                jobs_time_consumption_stats_dict['_successful_run_time_ratio']*100),
                    **result_dict
                )
        return result_str

    def run(self):
        tmp_log = logger_utils.make_logger(base_logger, method_name='SlowTaskAnalyzer.run')
        while True:
            # start
            tmp_log.info('start cycle')
            # make report file
            timeNow = datetime.datetime.utcnow()
            report_file = os.path.join(self.reportDir, 'slow_tasks_{0}.txt'.format(timeNow.strftime('%y%m%d_%H%M%S')))
            with open(report_file, 'w') as dump_file:
                # dump opening information
                dump_str = (
                            'Report created at {timestamp}\n\n'
                            'Parameters:\n'
                            'sinceHours = {sinceHours}\n'
                            'taskDurationMaxHours = {taskDurationMaxHours}\n'
                            'taskSuccefulRunTimeMinPercent = {taskSuccefulRunTimeMinPercent}\n'
                            'taskEachStatusMaxHours = {taskEachStatusMaxHours}\n'
                            'joblessIntervalMaxHours = {joblessIntervalMaxHours}\n'
                            'jobBadTimeMaxPercent = {jobBadTimeMaxPercent}\n'
                            'jobMaxHoursMap = {jobMaxHoursMap}\n'
                            '\n'
                            ).format(
                                    timestamp=timeNow.strftime('%y-%m-%d %H:%M:%S'),
                                    sinceHours=self.sinceHours,
                                    taskDurationMaxHours=self.taskDurationMaxHours,
                                    taskSuccefulRunTimeMinPercent=self.taskSuccefulRunTimeMinPercent,
                                    taskEachStatusMaxHours=self.taskEachStatusMaxHours,
                                    joblessIntervalMaxHours=self.joblessIntervalMaxHours,
                                    jobBadTimeMaxPercent=self.jobBadTimeMaxPercent,
                                    jobMaxHoursMap=self.jobMaxHoursMap,
                                )
                dump_file.write(dump_str)
                # candidate slow task attemps
                tmp_log.debug('fetching candidate slow task attemps created since {0} hours ago'.format(self.sinceHours))
                created_since = datetime.datetime.utcnow() - datetime.timedelta(hours=self.sinceHours)
                task_duration = datetime.timedelta(hours=self.taskDurationMaxHours)
                cand_ret_dict = self.dbProxy.slowTaskAttempsFilter01_ATM(created_since=created_since, prod_source_label=None, task_duration=task_duration)
                # filter to get slow task attemps
                tmp_log.debug('filtering slow task attemps')
                ret_dict = {}
                for k, v in cand_ret_dict.items():
                    jediTaskID, attemptNr = k
                    task_attempt_name = '{0}_{1:02}'.format(*k)
                    new_v = copy.deepcopy(v)
                    jobspec_list = self.dbProxy.slowTaskJobsInAttempt_ATM(jediTaskID=jediTaskID, attemptNr=attemptNr,
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
                    new_v['jobspec_list'] = jobspec_list
                    new_v['jobs_time_consumption_stats_dict'] = jobs_time_consumption_stats_dict
                    # more criteria of slow task
                    if successful_run_time_ratio*100 < self.taskSuccefulRunTimeMinPercent:
                        # successful run time occupied too little percentage of task duration
                        ret_dict[k] = new_v
                        tmp_log.debug('got a slow task attempt {0}'.format(task_attempt_name))
                n_slow_task_attempts = len(ret_dict)
                dump_str = 'got {0} slow task attemps: \n{1}\n'.format(n_slow_task_attempts, self._slow_task_attempts_display(ret_dict))
                dump_file.write(dump_str)
                tmp_log.debug(dump_str)
                # get culprits of slow task attempts
                tmp_log.debug('fetching culprits')
                dump_str = dump_str = '\n' + '='*64 + '\n' + 'Culprits of slowness:' + '\n\n'
                dump_file.write(dump_str)
                for k in sorted(ret_dict):
                    jediTaskID, attemptNr = k
                    dump_str = 'About jediTaskID={0} , attemptNr={1} \n\n'.format(jediTaskID, attemptNr)
                    dump_file.write(dump_str)
                    task_attempt_name = '{0}_{1:02}'.format(*k)
                    new_v = ret_dict[k]
                    slow_reason_set = set()
                    real_task_duration = new_v['real_task_duration']
                    jobspec_list = new_v['jobspec_list']
                    jobs_time_consumption_stats_dict = new_v['jobs_time_consumption_stats_dict']
                    # culprit task status (stuck long)
                    long_status_log_list = self._search_long_status(self._get_task_status_log(new_v['statusList']))
                    n_long_status = len(long_status_log_list)
                    bad_status_str = ','.join(sorted({ x['status'] for x in long_status_log_list }))
                    if n_long_status == 0:
                        tmp_log.debug('taskID_attempt={0} got 0 long status'.format(task_attempt_name))
                    else:
                        long_status_display_str = self._long_status_display(long_status_log_list)
                        dump_str = 'taskID_attempt={0} got {1} long status: \n{2}\n\n\n'.format(task_attempt_name, n_long_status, long_status_display_str)
                        dump_file.write(dump_str)
                        tmp_log.debug(dump_str)
                        slow_reason_set.add('TaskStatusLong')
                    # culprit intervals between jobs
                    bad_interval_list = self._search_bad_intervals(jobspec_list, new_v['startTime'])
                    n_bad_intervals = len(bad_interval_list)
                    if n_bad_intervals == 0:
                        tmp_log.debug('taskID_attempt={0} got 0 culprit intervals'.format(task_attempt_name))
                    else:
                        bad_intervals_display_str = self._bad_intervals_display(bad_interval_list)
                        slow_reason_set.add('JoblessIntervalLong')
                        dump_str = 'taskID_attempt={0} got {1} culprit intervals: \n{2}\n\n\n'.format(task_attempt_name, n_bad_intervals, bad_intervals_display_str)
                        dump_file.write(dump_str)
                        tmp_log.debug(dump_str)
                    # time consumption statistics of jobs
                    jobs_time_consumption_stats_display = self._jobs_time_consumption_stats_display(real_task_duration, jobs_time_consumption_stats_dict)
                    dump_str = 'taskID_attempt={0} time consumption stats of jobs: \n{1}\n'.format(task_attempt_name, jobs_time_consumption_stats_display)
                    dump_file.write(dump_str)
                    tmp_log.debug(dump_str)
                    # job symptom tags according to time consumption
                    job_slow_reason_set = self._bad_job_time_consumed_set(real_task_duration, jobs_time_consumption_stats_dict)
                    if not job_slow_reason_set:
                        tmp_log.debug('taskID_attempt={0} had no bad job symptom'.format(task_attempt_name))
                    else:
                        slow_reason_set |= job_slow_reason_set
                        dump_str = 'taskID_attempt={0} got bad job symptoms: {1}\n\n'.format(task_attempt_name, ','.join(sorted(job_slow_reason_set)))
                        dump_file.write(dump_str)
                        tmp_log.debug(dump_str)
                    # find some bad jobs as hint
                    pandaid_list = []
                    err_info_dict = {}
                    for jobspec in jobspec_list:
                        job_attr_dict = self._get_job_attr_dict(jobspec)
                        retVal = self._bad_job_qualify(job_attr_dict)
                        if retVal:
                            # qualified bad job
                            pandaid_list.append(jobspec.PandaID)
                            err_info = job_attr_dict['errorInfo']
                            if err_info in err_info_dict:
                                err_info_dict[err_info]['n_jobs'] += 1
                                err_info_dict[err_info]['waitDuration'] += job_attr_dict['waitDuration']
                                err_info_dict[err_info]['runDuration'] += job_attr_dict['runDuration']
                                err_info_dict[err_info]['priority'] += job_attr_dict['priority']
                            else:
                                err_info_dict[err_info] = {}
                                err_info_dict[err_info]['n_jobs'] = 1
                                err_info_dict[err_info]['waitDuration'] = job_attr_dict['waitDuration']
                                err_info_dict[err_info]['runDuration'] = job_attr_dict['runDuration']
                                err_info_dict[err_info]['priority'] = job_attr_dict['priority']
                    n_bad_jobs = len(pandaid_list)
                    if n_bad_jobs == 0:
                        tmp_log.debug('taskID_attempt={0} got 0 bad jobs'.format(task_attempt_name))
                    else:
                        bad_jobs_display_str = self._bad_jobs_display(pandaid_list, err_info_dict)
                        dump_str = 'taskID_attempt={0} got {1} bad jobs: \n{2}\n\n'.format(task_attempt_name, n_bad_jobs, bad_jobs_display_str)
                        dump_file.write(dump_str)
                        tmp_log.debug(dump_str)
                    # additional information about bad jobs
                    # additional_bad_job_info_msg_list = []
                    # additional_bad_job_info_list = bad_job_test_main(jobspec_list)
                    # for retVal, symptom_tag, retMsg in additional_bad_job_info_list:
                    #     if retVal:
                    #         slow_reason_set.add(symptom_tag)
                    #         msg = '{0}: {1}'.format(symptom_tag, retMsg)
                    #         additional_bad_job_info_msg_list.append(msg)
                    # if additional_bad_job_info_msg_list:
                    #     dump_str = 'taskID_attempt={0} additional info of culprit jobs: \n{1}\n\n\n'.format(task_attempt_name, '\n'.join(additional_bad_job_info_msg_list))
                    #     dump_file.write(dump_str)
                    #     tmp_log.debug(dump_str)
                    # summary of analysis of a task attempt
                    culprit_summary_str = 'taskID_attempt={task_attempt_name} slow reason: {slow_reasons}'.format(
                                                task_attempt_name=task_attempt_name,
                                                slow_reasons=' '.join(sorted(slow_reason_set)),
                                             )
                    tmp_log.info(culprit_summary_str)
                    dump_str = culprit_summary_str + '\n'
                    dump_file.write(dump_str)
                    dump_str = '\n' + '_'*64 + '\n\n'
                    dump_file.write(dump_str)
                tmp_log.debug('fetched culprits of all tasks')
                dump_str = 'End of report \n'
                dump_file.write(dump_str)
            # done
            tmp_log.info('done cycle')
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
        agent = SlowTaskAnalyzer()
        agent.run()
