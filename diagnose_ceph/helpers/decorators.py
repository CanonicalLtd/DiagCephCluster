from functools import wraps, partial
import signal

from exceptions import TimeoutError


def timeout(seconds=20):

    def decorator(func):
        def _handle_timeout(command, signum, frame):
            raise TimeoutError("'%s' command did not return" % command)

        def wrapper(*args, **kwargs):
            if len(args) >= 3:
                signal.signal(signal.SIGALRM, partial(_handle_timeout,
                                                      args[2]))
            else:
                signal.signal(signal.SIGALRM, partial(_handle_timeout, None))
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator
