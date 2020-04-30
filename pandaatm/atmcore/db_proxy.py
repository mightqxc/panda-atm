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

    def slowTaskAttempsFilter01_ATM(self, created_since: datetime.datetime, prod_source_label: str = 'user', task_duration: datetime.timedelta = datetime.timedelta(hours=168)) -> dict :
        """
        First filter to get possible slow tasks
        """
        comment = ' /* atmcore.db_proxy.slowTaskAttempsFilter01_ATM */'
        method_name = self.getMethodName(comment)
        # method_name += " < jediTaskID={0} >".format(jediTaskID)
        tmp_log = logger_utils.make_logger(base_logger, method_name=method_name)
        tmp_log.debug('start')
        try:
            taskAttempsDict = {}
            retDict = {}
            # sql to get tasks with the first filter
            sqlT = (
                    "SELECT jediTaskID,creationDate "
                    "FROM ATLAS_PANDA.JEDI_Tasks "
                    "WHERE prodSourceLabel=:prodSourceLabel AND (CAST(endTime AS TIMESTAMP) - creationDate) >:taskDurationMax AND creationDate>=:creationDateMin "
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
            varMap[':taskDurationMax'] = task_duration
            self.cur.execute(sqlT + comment, varMap)
            tmpTasksRes = self.cur.fetchall()
            # loop over tasks to parse status log
            for jediTaskID, creationDate in tmpTasksRes:
                varMap = dict()
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlSL + comment, varMap)
                tmpSLRes = self.cur.fetchall()
                # parse status log
                # (jediTaskID,attemptNr): {startTime, endTime, attemptDuration, finalStatus, statusList}
                attemptNr = 1
                toGetAttempt = True
                for modificationTime, status in tmpSLRes:
                    if toGetAttempt:
                        taskAttempsDict[(jediTaskID, attemptNr)] = {}
                        taskAttempsDict[(jediTaskID, attemptNr)]['startTime'] = modificationTime
                        taskAttempsDict[(jediTaskID, attemptNr)]['statusList'] = []
                        toGetAttempt = False
                    taskAttempsDict[(jediTaskID, attemptNr)]['statusList'].append((status, modificationTime))
                    taskAttempsDict[(jediTaskID, attemptNr)]['finalStatus'] = status
                    if status in ('finished', 'done', 'failed', 'aborted', 'broken'):
                        taskAttempsDict[(jediTaskID, attemptNr)]['endTime'] = modificationTime
                        try:
                            taskAttempsDict[(jediTaskID, attemptNr)]['attemptDuration'] = modificationTime - taskAttempsDict[(jediTaskID, attemptNr)]['startTime']
                        except KeyError:
                            pass
                        toGetAttempt = True
                        attemptNr += 1
                # filter for return dict
                for k, v in taskAttempsDict.items():
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

    def slowTaskJobsInAttempt_ATM(self, jediTaskID: int, attemptNr: int, attempt_start: datetime.datetime, attempt_end: datetime.datetime) -> list :
        """
        Jobs of a slow task attempt
        """
        comment = ' /* atmcore.db_proxy.slowTaskJobsInAttempt_ATM */'
        method_name = self.getMethodName(comment)
        method_name += ' < jediTaskID={0} attemptNr={1} > '.format(jediTaskID, attemptNr)
        tmp_log = logger_utils.make_logger(base_logger, method_name=method_name)
        tmp_log.debug('start')
        try:
            # sql to get archived jobs
            sqlJA1 = (
                    'SELECT {job_columns} '
                    'FROM ATLAS_PANDAARCH.JOBSARCHIVED '
                    'WHERE jediTaskID=:jediTaskID AND creationTime>=:attempt_start AND creationTime<=:attempt_end '
                ).format(job_columns=str(JobSpec.columnNames()))
            sqlJA2 = (
                    'SELECT {job_columns} '
                    'FROM ATLAS_PANDA.JOBSARCHIVED4 '
                    'WHERE jediTaskID=:jediTaskID AND creationTime>=:attempt_start AND creationTime<=:attempt_end '
                ).format(job_columns=str(JobSpec.columnNames()))
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
        Attemps of file processing of a slow task
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
