# default library
import sys
import time
import json
import threading
import logging
import logging.handlers
from datetime import datetime, timedelta

# third-party library

# self-made library
sys.path.append('..')
from master.spark_unit import *
from publib.SparkConn import *

global timer
timer = None


def tick(i, func, *args, **kwargs):
    global timer
    if timer:
        timer.finished.wait(i)
        timer.function(*args, **kwargs)
    else:
        timer = threading.Timer(i, func, *args, **kwargs)
        timer.start()


class Application:

    def __init__(self):
        self.i = 0

    # # signal sent
    def periodical_signal(self):
        global timer
        print('123')
        self.i += 1
        if self.i > 10:
            timer.cancel()
            return
        tick(2.0, self.periodical_signal)

    # main body
    def run(self):
        tick(2.0, self.periodical_signal)


    def __del__(self):
        global timer
        if timer:
            timer.cancel()


# instantiation
app = Application()
app.run()
