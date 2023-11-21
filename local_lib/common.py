import logging
import sys
from io import StringIO

def set_up_logging():

    class CallCounted:
        """Decorator to determine number of calls for a method"""

        def __init__(self, method):
            self.method = method
            self.counter = 0

        def __call__(self, *args, **kwargs):
            self.counter += 1
            return self.method(*args, **kwargs)
    log_stream = StringIO()
    root_log = logging.getLogger()
    root_log.error = CallCounted(root_log.error)
    root_log.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_log.addHandler(handler)

    handler.setStream(sys.stdout)
    handler.setLevel(logging.INFO)
    root_log.addHandler(handler)

    return log_stream, root_log


