import logging
import smtplib
import sys

from email.message import EmailMessage
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


def send_email(recipients: list, subject: str, body: str, smtp_config: dict,
                     from_address: str = None, debug: bool = True):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = smtp_config['USERNAME'] if from_address is None else from_address
    msg['To'] = recipients
    msg.set_content(body)
    server = smtplib.SMTP(smtp_config['HOST'], smtp_config['PORT'])
    if debug:
        server.set_debuglevel(1)
    server.starttls()
    server.login(smtp_config['USERNAME'], smtp_config['PASSWORD'])
    server.send_message(msg)
    server.quit()


def strip_value(s):
    try:
        return s.strip()
    except:
        return s