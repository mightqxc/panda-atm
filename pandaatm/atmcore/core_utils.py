import os
import datetime
import pathlib
import threading
import sqlite3

#=== Functions =================================================

def timedelta_parse_dict(delta_t: datetime.timedelta) -> dict:
    total_seconds = delta_t.total_seconds()
    seconds = int(total_seconds)
    microseconds = int((total_seconds - seconds)*10**6)
    days, seconds = divmod(seconds, 60*60*24)
    hours, seconds = divmod(seconds, 60*60)
    minutes, seconds = divmod(seconds, 60)
    ret_dict = {
                    'days': days,
                    'hours': hours,
                    'minutes': minutes,
                    'seconds': seconds,
                    'microseconds': microseconds,
                    'total_seconds': total_seconds,
                    'str_dcolon': f'{days}d {hours:02}:{minutes:02}:{seconds:02}',
                }
    return ret_dict


#=== Classes ===================================================

# object of context manager for db proxy
class SQLiteProxy(object):

    def _connect(self):
        self.con = sqlite3.connect( self.db_file,
                                    detect_types=(sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES),
                                    check_same_thread=False)
        self.cur = self.con.cursor()

    def _connect_readonly(self):
        self.con = sqlite3.connect( '{0}?mode=ro'.format(pathlib.PurePath(self.db_file).as_uri()),
                                    detect_types=(sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES),
                                    check_same_thread=False, uri=True)
        self.cur = self.con.cursor()

    def __init__(self, db_file, readonly=False):
        self.db_file = os.path.normpath(db_file)
        self.lock = threading.Lock()
        if readonly:
            self._connect_readonly()
        else:
            self._connect()

    def get_proxy(self):
        proxy_obj = SQLiteProxyObj(proxy=self, to_lock=True)
        return proxy_obj

    def close(self):
        with self.lock:
            self.cur.close()
            self.con.close()


# object of context manager for sqlite proxy for lock
class SQLiteProxyObj(object):

    def __init__(self, proxy, to_lock=False):
        self.proxy = proxy
        self.to_lock = to_lock

    def __enter__(self):
        if self.to_lock:
            self.proxy.lock.acquire()
        return self.proxy.cur

    def __exit__(self, type, value, traceback):
        self.proxy.con.commit()
        if self.to_lock:
            self.proxy.lock.release()
        self.proxy = None
