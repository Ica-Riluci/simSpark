import logging
import logging.handlers
import threading
import time
import datetime
import json
from daemon import runner
import sys

sys.path.append('publib')

from publib import connection

class SparkUnit:
    spark_unit_count = 0
    def __init__(self, add, po):
        self.address = add
        self.port = po
        self.lasthb = datetime.datetime.now()
        self.alive = True
        SparkUnit.spark_unit_count += 1
# this class denotes all kind of units managed by master:
# application, worker, driver

def load_config(logs):
    config = {
        'master_port' : 7077,
        'webui_port' : 8080,
        'worker_timeout' : 60000,
        'spread_out' : True,
        'default_core' : -1,
        'reaper_iteration' : 15,
        'executor_max_retries' : 10
    }
    try:
        with open('master_config.json', 'r') as jsoninput:
            inp = json.load(jsoninput)
        for k in config.keys():
            if k in inp.keys():
                config[k] = inp[k]
    except IOError:
        logs.warning('Failed to read configuration. Use default instead.')
    return config
        

class MasterDaemon:
    def __init__(self):
        self.stdin_path = '/tmp/simSpark'
        self.stdout_path = '/tmp/simSpark'
        self.stderr_path = '/tmp/simSpark'
        self.pidfile_path =  '/tmp/simSpark.pid'
        self.pidfile_timeout = 5

    def check_worker_timeout(self):
        # send msg

        timer = threading.Timer(2.0, self.check_worker_timeout)
        timer.start()
        # restart timer

    def process(self, msg):
        if msg['type'] == 'check_worker_timeout':
            pass
        elif msg['type'] == 'reg_worker':
            pass
        elif msg['type'] == 'reg_app':
            pass
        elif msg['type'] == 'exec_stage_changed':
            pass
        elif msg['type'] == 'rm_app':
            pass
        elif msg['type'] == 'kill_exec':
            pass
        elif msg['type'] == 'driver_state_changed':
            pass
        elif msg['type'] == 'worker_hb':
            pass
        elif msg['type'] == 'master_change_ack':
            pass
        elif msg['type'] == 'worker_schedule_state_resp':
            pass
        elif msg['type'] == 'worker_latest_state':
            pass
        elif msg['type'] == 'unreg_app':
            pass
        elif msg['type'] == 'req_submit_driver':
            pass
        elif msg['type'] == 'req_kill_driver':
            pass
        elif msg['type'] == 'req_driver_status':
            pass
        elif msg['type'] == 'req_master_status':
            pass
        elif msg['type'] == 'req_exec':
            pass
        elif msg['type'] == 'kill_exec':
            pass


    def run(self):
        logs = logging.getLogger('simSparkLog')
        logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark.log',maxBytes=10000000,backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        logs.addHandler(fh)

        config = load_config(logs)
        if config['default_core'] < 1 and config['default_core'] != -1:
            logs.critical('Default core(s) assigned must be positive.')
            return
        # fetch configuration

        # app_id = []
        # app_wl = []
        # app = []
        # app_ad = []
        # app_completed = []
        # app_next = 0

        # workers = []
        # worker_id = []
        # worker_ad = []
        
        # drivers = []
        # driver_completed = []
        # driver_wl = []
        # recording structure

        timer = threading.Timer(2.0, self.check_worker_timeout)
        timer.start()

        # listener = SparkConn('localhost', config['master_port'])

        # onStart

        while True:
            # msg = listener.accept()
            # self.process(json.loads(msg['value']))
            pass
        # listening - this part should contains how the daemon listens from socket $master_socket and the result

app = MasterDaemon()
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()