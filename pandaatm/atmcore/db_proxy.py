import os
import sys
import re
import copy
import logging
import datetime
import traceback
import itertools

import cx_Oracle

from pandaatm.atmconfig import atm_config
from pandaatm.atmcore import core_utils
from pandaatm.atmutils.generic_utils import TaskAttempt

from pandacommon.pandalogger import logger_utils

from pandaserver.taskbuffer import OraDBProxy
from pandaserver.taskbuffer.JobSpec  import JobSpec

# from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
# from pandajedi.jedicore.JediFileSpec import JediFileSpec
# from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
# from pandajedi.jedicore.InputChunk import InputChunk
# from pandajedi.jedicore.MsgWrapper import MsgWrapper


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])
OraDBProxy._logger = base_logger


class DBProxy(OraDBProxy.DBProxy):

    # constructor
    def __init__(self, useOtherError=False):
        OraDBProxy.DBProxy.__init__(self, useOtherError)

    # connect to DB
    def connect(self, dbhost=atm_config.db.dbhost, dbpasswd=atm_config.db.dbpasswd,
                dbuser=atm_config.db.dbuser, dbname=atm_config.db.dbname,
                dbtimeout=None, reconnect=False):
        return OraDBProxy.DBProxy.connect(self, dbhost=dbhost, dbpasswd=dbpasswd,
                                                     dbuser=dbuser, dbname=dbname,
                                                     dbtimeout=dbtimeout, reconnect=reconnect)

    # extract method name from comment
    def getMethodName(self, comment):
        tmpMatch = re.search('([^ /*]+)', comment)
        if tmpMatch is not None:
            methodName = tmpMatch.group(1).split('.')[-1]
        else:
            methodName = comment
        return methodName

    # check if exception is from NOWAIT
    def isNoWaitException(self, errValue):
        oraErrCode = str(errValue).split()[0]
        oraErrCode = oraErrCode[:-1]
        if oraErrCode == 'ORA-00054':
            return True
        return False

    # dump error message
    def dumpErrorMessage(self, tmpLog, methodName=None, msgType=None):
        # error
        errtype,errvalue = sys.exc_info()[:2]
        if methodName is not None:
            errStr = methodName
        else:
            errStr = ''
        errStr += ': {0} {1}'.format(errtype.__name__, errvalue)
        errStr.strip()
        errStr += traceback.format_exc()
        if msgType == 'debug':
            tmpLog.debug(errStr)
        else:
            tmpLog.error(errStr)

    #====================================================================

    def slowTaskAttemptsFilter01_ATM(self,
                                    created_since: datetime.datetime,
                                    created_before=None,
                                    prod_source_label: str = 'user',
                                    gshare=None,
                                    task_duration=None,
                                    ) -> dict :
        """
        First filter to get possible slow tasks
        """
        comment = ' /* atmcore.db_proxy.slowTaskAttemptsFilter01_ATM */'
        method_name = self.getMethodName(comment)
        # method_name += " < jediTaskID={0} >".format(jediTaskID)
        tmp_log = logger_utils.make_logger(base_logger, method_name=method_name)
        tmp_log.debug('start')
        try:
            taskAttemptsDict = {}
            retDict = {}
            # sql to get tasks with the first filter
            sqlT = (
                    "SELECT jediTaskID,creationDate,userName "
                    "FROM ATLAS_PANDA.JEDI_Tasks "
                    "WHERE prodSourceLabel=:prodSourceLabel "
                        "AND creationDate>=:creationDateMin "
                        "{created_before_filter} "
                        "{gshare_filter} "
                        "{task_duration_filter} "
                    "ORDER BY jediTaskID DESC "
                )
            # sql to get task status log
            sqlSL = (
                    'SELECT modificationTime,status '
                    'FROM ATLAS_PANDA.Tasks_StatusLog '
                    'WHERE jediTaskID=:jediTaskID '
                    'ORDER BY modificationTime '
                )
            # get tasks
            varMap = dict()
            varMap[':prodSourceLabel'] = prod_source_label
            varMap[':creationDateMin'] = created_since
            created_before_filter = ''
            gshare_filter = ''
            if created_before is not None:
                varMap[':creationDateMax'] = created_before
                created_before_filter = 'AND creationDate<:creationDateMax'
            if gshare is not None:
                varMap[':gshare'] = gshare
                gshare_filter = 'AND gshare=:gshare'
            if task_duration is not None:
                varMap[':taskDurationMax'] = task_duration
                task_duration_filter = 'AND (CAST(endTime AS TIMESTAMP) - creationDate) >:taskDurationMax '
            sqlT = sqlT.format( created_before_filter=created_before_filter,
                                gshare_filter=gshare_filter)
            self.cur.execute(sqlT + comment, varMap)
            tmpTasksRes = self.cur.fetchall()
            tmp_log.debug('got tasks to parse')
            # loop over tasks to parse status log
            for jediTaskID, creationDate, userName in tmpTasksRes:
                varMap = dict()
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlSL + comment, varMap)
                tmpSLRes = self.cur.fetchall()
                # parse status log
                # (jediTaskID,attemptNr): {startTime, endTime, attemptDuration, finalStatus, statusList, userName}
                attemptNr = 1
                toGetAttempt = True
                for modificationTime, status in tmpSLRes:
                    if toGetAttempt:
                        taskAttemptsDict[(jediTaskID, attemptNr)] = {}
                        taskAttemptsDict[(jediTaskID, attemptNr)]['startTime'] = modificationTime
                        taskAttemptsDict[(jediTaskID, attemptNr)]['statusList'] = []
                        taskAttemptsDict[(jediTaskID, attemptNr)]['userName'] = userName
                        toGetAttempt = False
                    taskAttemptsDict[(jediTaskID, attemptNr)]['statusList'].append((status, modificationTime))
                    taskAttemptsDict[(jediTaskID, attemptNr)]['finalStatus'] = status
                    if status in ('finished', 'done', 'failed', 'aborted', 'broken'):
                        taskAttemptsDict[(jediTaskID, attemptNr)]['endTime'] = modificationTime
                        try:
                            taskAttemptsDict[(jediTaskID, attemptNr)]['attemptDuration'] = modificationTime - taskAttemptsDict[(jediTaskID, attemptNr)]['startTime']
                        except KeyError:
                            pass
                        toGetAttempt = True
                        attemptNr += 1
                # filter for return dict
                for k, v in taskAttemptsDict.items():
                    if 'attemptDuration' in v and v['attemptDuration'] > task_duration:
                        retDict[k] = v
            tmp_log.debug('done, got {0} slow task attempts'.format(len(retDict)))
            # return
            return retDict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return None

    def getTaskAttempts_ATM(self,
                            created_since: datetime.datetime,
                            created_before=None,
                            prod_source_label: str = 'user',
                            gshare=None,
                            attempt_duration=None,
                            ) -> dict :
        """
        Query task attempts by timestamps
        """
        comment = ' /* atmcore.db_proxy.getTaskAttempts_ATM */'
        method_name = self.getMethodName(comment)
        tmp_log = logger_utils.make_logger(base_logger, method_name=method_name)
        tmp_log.debug('start')
        try:
            taskAttemptsDict = {}
            retDict = {}
            # sql to get attempt from task status log and tasks table
            sqlSLT = (
                    'SELECT sl.jediTaskID,sl.modificationTime,sl.status,t.userName '
                    'FROM ATLAS_PANDA.Tasks_StatusLog sl, ATLAS_PANDA.JEDI_Tasks t '
                    'WHERE sl.jediTaskID=t.jediTaskID '
                        "AND t.prodSourceLabel=:prodSourceLabel "
                        "AND t.modificationTime>=:creationDateMin "
                        "{created_before_filter} "
                        "{gshare_filter} "
                    'ORDER BY sl.jediTaskID, sl.modificationTime '
                )
            # get tasks
            varMap = dict()
            varMap[':prodSourceLabel'] = prod_source_label
            varMap[':creationDateMin'] = created_since
            created_before_filter = ''
            gshare_filter = ''
            if created_before is not None:
                varMap[':creationDateMax'] = created_before
                created_before_filter = 'AND t.creationDate<:creationDateMax'
            if gshare is not None:
                varMap[':gshare'] = gshare
                gshare_filter = 'AND t.gshare=:gshare'
            sqlSLT = sqlSLT.format( created_before_filter=created_before_filter,
                                    gshare_filter=gshare_filter)
            self.cur.execute(sqlSLT + comment, varMap)
            tmpTasksRes = self.cur.fetchall()
            tmp_log.debug('got task status logs to parse')
            # loop over task status logs to parse task attempts
            task_attempt_record_dict = {}
            for jediTaskID, modificationTime, status, userName in tmpTasksRes:
                if jediTaskID not in task_attempt_record_dict:
                    # new task, mark attempt = 1
                    task_attempt_record_dict[jediTaskID] = 1
                # current attempt number of the task
                attemptNr = task_attempt_record_dict[jediTaskID]
                key = (jediTaskID, attemptNr)
                # fill the dict with the task attempt
                if key not in taskAttemptsDict:
                    # new task attempt
                    taskAttemptsDict[key] = TaskAttempt(jediTaskID=jediTaskID,
                                                        attemptNr=attemptNr,
                                                        startTime=modificationTime,
                                                        userName=userName)
                # current task attempt
                task_attempt = taskAttemptsDict[key]
                # update task attempt
                task_attempt.update_status(status=status, modificationTime=modificationTime)
                # check whether the task attempt is complete
                if task_attempt.is_complete():
                    # increase attemptNr fot the task
                    task_attempt_record_dict[jediTaskID] += 1
            # filter for return dict
            for key, task_attempt in taskAttemptsDict.items():
                if task_attempt.is_complete() \
                        and task_attempt.startTime >= created_since \
                        and (created_before is None or task_attempt.startTime < created_before):
                    retDict[key] = task_attempt
            tmp_log.debug('done, got {0} task attempts'.format(len(retDict)))
            # return
            return retDict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return None

    def slowTaskJobsInAttempt_ATM(self, jediTaskID: int, attemptNr: int,
                                    attempt_start: datetime.datetime, attempt_end: datetime.datetime,
                                    concise=False) -> list :
        """
        Jobs of a slow task attempt
        """
        comment = ' /* atmcore.db_proxy.slowTaskJobsInAttempt_ATM */'
        method_name = self.getMethodName(comment)
        method_name += ' < jediTaskID={0} attemptNr={1} > '.format(jediTaskID, attemptNr)
        tmp_log = logger_utils.make_logger(base_logger, method_name=method_name)
        tmp_log.debug('start')
        try:
            if concise:
                important_attrs = [ 'PandaID', 'jediTaskID', 'jobStatus', 'actualCoreCount',
                                    'creationTime', 'startTime', 'endTime']
                job_columns = ','.join(important_attrs)
            else:
                job_columns = str(JobSpec.columnNames())
            # sql to get archived jobs
            sqlJA1 = (
                    'SELECT {job_columns} '
                    'FROM ATLAS_PANDAARCH.JOBSARCHIVED '
                    'WHERE jediTaskID=:jediTaskID AND creationTime>=:attempt_start AND creationTime<=:attempt_end '
                ).format(job_columns=job_columns)
            sqlJA2 = (
                    'SELECT {job_columns} '
                    'FROM ATLAS_PANDA.JOBSARCHIVED4 '
                    'WHERE jediTaskID=:jediTaskID AND creationTime>=:attempt_start AND creationTime<=:attempt_end '
                ).format(job_columns=job_columns)
            # get jobs
            varMap = dict()
            varMap[':jediTaskID'] = jediTaskID
            varMap[':attempt_start'] = attempt_start
            varMap[':attempt_end'] = attempt_end
            self.cur.execute(sqlJA1 + comment, varMap)
            tmpJRes1 = self.cur.fetchall()
            self.cur.execute(sqlJA2 + comment, varMap)
            tmpJRes2 = self.cur.fetchall()
            tmpJRes = itertools.chain(tmpJRes1, tmpJRes2)
            # add jobspecs in list
            pandaidSet = set()
            retList = []
            for one_job in tmpJRes:
                jobspec = JobSpec()
                if concise:
                    for attr, value in zip(important_attrs, one_job):
                        setattr(jobspec, attr, value)
                else:
                    jobspec.pack(one_job)
                pandaid = jobspec.PandaID
                # prevent duplicate jobspec from different tables
                if pandaid not in pandaidSet:
                    pandaidSet.add(pandaid)
                    retList.append(jobspec)
            # return
            tmp_log.debug('done, got {0} jobs'.format(len(retList)))
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return None






    def slowTaskFileAttempts_ATM(self, jediTaskID: int):
        """
        Attempts of file processing of a slow task
        """
        comment = ' /* atmcore.db_proxy.slowTaskFileAttempts_ATM */'
        method_name = self.getMethodName(comment)
        # method_name += " < jediTaskID={0} >".format(jediTaskID)
        tmp_log = logger_utils.make_logger(base_logger, method_name=method_name)
        tmp_log.debug('start')
        try:
            # sql to get tasks with the first filter
            sqlT = (
                    'WITH dd AS ('
                        'SELECT DISTINCT ff.PandaID, MIN(ff.FileID) OVER (PARTITION BY ff.PandaID) AS fid '
                        'FROM ATLAS_PANDAARCH.FILESTABLE_ARCH ff INNER JOIN ATLAS_PANDA.JEDI_DATASETS ds ON ff.datasetID=ds.datasetID AND ff.jediTaskID=ds.jediTaskID '
                        'WHERE ds.type="input" AND ds.jediTaskID=:jediTaskID) '
                    'SELECT dd.fid,pp.PandaID,pp.attemptNr,creationTime '
                    'FROM dd INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED pp ON pp.PandaID=dd.PandaID '
                    'ORDER BY dd.fid,pp.attemptNr,pp.PandaID '
                )
            # get tasks
            varMap = dict()
            varMap[':jediTaskID'] = jediTaskID
            varMap[':status'] = 'pending'
            self.cur.execute(sqlT + comment, varMap)
            nDone = self.cur.rowcount
            # return
            tmp_log.debug('kicked with {0}'.format(nDone))
            return nDone
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmp_log)
            return None
