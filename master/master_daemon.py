import logging
import logging.handlers
import threading
import time
import json
from daemon import runner

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
        

class MasterDaemon():
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

        app_id = []
        app_wl = []
        app = []
        app_ad = []
        app_completed = []
        app_next = 0

        workers = []
        worker_id = []
        worker_ad = []
        
        drivers = []
        driver_completed = []
        driver_wl = []
        # recording structure

        timer = threading.Timer(2.0, self.check_worker_timeout)
        timer.start()
        
        # onStart

        '''
        socket conneting
        '''
        while True:
            '''
            socking listening and processing
            '''
            pass
        # listening - this part should contains how the daemon listens from socket $master_socket and the result