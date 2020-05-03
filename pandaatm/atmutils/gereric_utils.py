

def get_task_attempt_name(jediTaskID, attemptNr):
    """
    get task attempt name (encodable string) from jediTaskID and attemptNr
    """
    task_attempt_name = f'{jediTaskID}#{attemptNr}'
    return task_attempt_name

def get_taskid_atmptn(task_attempt_name):
    """
    get jediTaskID and attemptNr from task attempt name
    """
    tmp_list = task_attempt_name.split('#')
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
