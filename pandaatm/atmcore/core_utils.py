import datetime


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
