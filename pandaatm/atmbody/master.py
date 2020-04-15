import os
import sys
import pwd
import grp
import time
import signal
import argparse
import datetime
import multiprocessing

import daemon

from pandaatm.atmconfig import atm_config


# master class of ATM which runs the main process
class AtmMaster(object):

    # constructor
    def __init__(self):
        pass

    # spawn a proc to have own file descriptors
    def launcher(self, moduleName, *args, **kwargs):
        # import module
        mod = __import__(moduleName)
        for subModuleName in moduleName.split('.')[1:]:
            mod = getattr(mod, subModuleName)
        # launch
        timeNow = datetime.datetime.utcnow()
        print("{0} {1}: INFO    start {2} with pid={3}".format(str(timeNow),
                                                               moduleName,
                                                               'launcher',
                                                               os.getpid()))
        mod.launcher(*args, **kwargs)



    # convert config parameters
    def convParams(self,itemStr):
        items = itemStr.split(':')
        newItems = []
        for item in items:
            if item == '':
                newItems.append(None)
            elif ',' in item:
                newItems.append(item.split(','))
            else:
                try:
                    newItems.append(int(item))
                except Exception:
                    newItems.append(item)
        return newItems



    # main process
    def start(self):
        # the list of agent processes
        procList = []
        # setup agents
        # for agent_type in something: # FIXME
        #     for agent_each in agent_type: # FIXME
        #         for i_proc in range(n_proc): # FIXME
        #             parent_conn, child_conn = multiprocessing.Pipe()
        #             proc = multiprocessing.Process(name=some_name, target=self.launcher, args=some_args_tuple)
        #             proc.start()
        #             procList.append(proc)
        # slow_task_analyzer agent
        parent_conn, child_conn = multiprocessing.Pipe()
        proc = multiprocessing.Process(name='slow_task_analyzer', target=self.launcher, args=('pandaatm.atmbody.slow_task_analyzer',))
        proc.start()
        procList.append(proc)
        # testing agent
        # parent_conn, child_conn = multiprocessing.Pipe()
        # proc = multiprocessing.Process(name='testing_agent', target=self.launcher, args=('pandaatm.atmbody.testing_agent',))
        # proc.start()
        # procList.append(proc)
        # check initial failures
        time.sleep(5)
        for proc in procList:
            if not proc.is_alive():
                timeNow = datetime.datetime.utcnow()
                print('{0} {1}: ERROR  name={2}, pid={3} died in startup'.format(
                                                                            str(timeNow),
                                                                            self.__class__.__name__,
                                                                            proc.name,
                                                                            proc.pid))
                os.killpg(os.getpgrp(), signal.SIGKILL)
        # join
        for proc in procList:
            proc.join()



# kill whole process
def catch_sig(sig,frame):
    # kill
    os.killpg(os.getpgrp(),signal.SIGKILL)



# main
if __name__ == "__main__":
    # parse option
    parser = argparse.ArgumentParser(prog='atm-master', add_help=True)
    parser.add_argument('--pid', action='store', dest='pid', default=None, help='pid filename')
    arguments = parser.parse_args()
    uid = pwd.getpwnam(atm_config.master.uname).pw_uid
    gid = grp.getgrnam(atm_config.master.gname).gr_gid
    timeNow = datetime.datetime.utcnow()
    print("{0} AtmMaster: INFO    start".format(str(timeNow)))
    # make daemon context
    dc = daemon.DaemonContext(stdout=sys.stdout,
                              stderr=sys.stderr,
                              uid=uid,
                              gid=gid)
    with dc:
        # record PID
        with open(arguments.pid, 'w') as pidFile:
            pidFile.write('{0}'.format(os.getpid()))
        # set handler
        signal.signal(signal.SIGINT, catch_sig)
        signal.signal(signal.SIGHUP, catch_sig)
        signal.signal(signal.SIGTERM, catch_sig)
        # start master
        master = AtmMaster()
        master.start()
