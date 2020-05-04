#=== Functions =================================================

def get_task_attempt_key_name(jediTaskID, attemptNr):
    """
    get task attempt key name (encodable string) from jediTaskID and attemptNr
    """
    key_name = f'{jediTaskID}#{attemptNr}'
    return key_name

def get_taskid_atmptn(key_name):
    """
    get jediTaskID and attemptNr from task attempt key name
    """
    tmp_list = key_name.split('#')
    jediTaskID = int(tmp_list[0])
    attemptNr = int(tmp_list[1])
    return jediTaskID, attemptNr

def get_change_of_set(orig_set, obj, mode):
    """
    get a tuple change_tuple=(increased, decreased), assuming one changes the set with the object (add or remove)
    """
    if mode == '+':
        if obj not in orig_set:
            # new object to add to the set
            return (obj, None)
    elif mode == '-':
        if obj in orig_set:
        # existing object to remove from the set
            return (None, obj)
    # set unchanged
    return (None, None)

def update_set_by_change_tuple(orig_set, change_tuple):
    """
    update the set according to change_tuple
    """
    to_add = change_tuple[0]
    to_discard = change_tuple[1]
    if to_add is not None:
        orig_set.add(to_add)
    if to_discard is not None:
        orig_set.discard(to_discard)


#=== Classes ===================================================

class TaskAttempt(object):
    """
    Task Attempt object
    """

    __slots__ = [
            'jediTaskID',
            'attemptNr',
            'keyName',
            'userName',
            'startTime',
            'endTime',
            'attemptDuration',
            'finalStatus',
            'statusList',
        ]

    def __init__(self, jediTaskID, attemptNr, startTime,
                    endTime=None, finalStatus=None, statusList=None, userName=None):
        # initialize
        self._is_complete = False
        # fill attributes
        self.jediTaskID = jediTaskID
        self.attemptNr = attemptNr
        self.startTime = startTime
        self.endTime = endTime
        self.statusList = statusList
        self.userName = userName
        # compute attributes
        if self.statusList is None:
            self.statusList = []
        self.keyName = get_task_attempt_key_name(self.jediTaskID, self.attemptNr)
        if endTime is not None and finalStatus is not None:
            self._set_complete(finalStatus=finalStatus, endTime=endTime)

    def _set_complete(self, finalStatus, endTime):
        """
        set the task attempt complete
        """
        if endTime is None:
            raise RuntimeError('TaskAttempt: cannot set complete with endTime = None')
        elif finalStatus not in ('finished', 'done', 'failed', 'aborted', 'broken'):
            raise RuntimeError('TaskAttempt: cannot set complete with invalid finalStatus')
        else:
            self.finalStatus = finalStatus
            self.endTime = endTime
            self.attemptDuration = self.endTime - self.startTime
            self._is_complete = True

    def update_status(self, status, modificationTime):
        """
        whether the task attempt is complete; i.e. terminated with a final status
        """
        self.attemptDuration = modificationTime - self.startTime
        self.statusList.append((status, modificationTime))
        if status in ('finished', 'done', 'failed', 'aborted', 'broken'):
            self._set_complete(finalStatus=status, endTime=modificationTime)

    def is_complete(self):
        """
        whether the task attempt is complete; i.e. terminated with a final status
        """
        return self._is_complete
