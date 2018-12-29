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

class Application:
    
    def __init__(self):
        # initialize logger
        self.logs = logging.getLogger('simSparkLog')
        self.logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark_master.log',maxBytes=10000000,backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logs.addHandler(fh)

        self.logs.info('simSpark master has been awaken.')
        self.config = self.load_config()
        if self.config['default_core'] < 1 and self.config['default_core'] != -1:
            self.logs.critical('Default core(s) assigned must be positive.')
            sys.exit(1)
        self.apps = []
        self.workers = []
        self.drivers = []
        self.executors = []

    # load configuration
    def load_config(self):
        self.logs.info('<master_config.json> is about to be loaded.')
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
            self.logs.warning('Failed to read configuration. Use default instead.')
        return config
    
    # signal sent
    def periodical_signal(self):
        msg = self.wrap_msg('localhost', self.config['master_port'], 'check_worker_TO', None)
        self.listener.sendMessage(msg)
        timer = threading.Timer(2.0, self.periodical_signal)
        timer.start()

    def feedback_application(self, app):
        value = {
            'id' : app.id,
            'executor_list' : app.executor_list
        }
        self.listener.sendMessage(self.wrap_msg(app.host, app.port, 'resource_update', value))

    # wrap the message
    def wrap_msg(self, address, port, type, value):
        raw = {
            'type' : type,
            'value' : value
        }
        wrapped = {
            'host' : address,
            'port' : port,
            'value' : raw
        }
        return wrapped

    # functional components
    def search_driver_by_id(self, id):
        for d in self.drivers:
            if d.driver_id == id:
                return self.drivers.index(d)
        return None

    # reaction to message
    def check_workers_heartbeat(self):
        for worker in self.workers:
            if worker.alive:
                if worker.hearbeat_expired(self.config['worker_timeout']):
                    self.logs.warning('Worker %d is out of contact.' % (worker.id))
                    worker.alive = False
            else:
                if worker.dead(self.config['worker_timeout'], self.config['reap_iteration']):
                    self.logs.warning('Worker %d will be buried for out of contact after several iterations.' % (worker.id))
                    self.workers.remove(worker)

    def register_application(self, app):
        self.logs.info('Request for registration of application %s received.' % (app['name']))
        driver_idx = self.search_driver_by_id(app['did'])
        if driver_idx:
            if self.drivers[driver_idx].app_id:
                self.logs.critical('An application is already binded to driver %d.' % (app['did']))
                return
            new_app = ApplicationUnit(app['host'], app['port'], app['name'], app['did'], app['exec_req'])
            self.apps.append(new_app)
            self.logs.info('Application %s is binded to driver %d using id %d.' % (app['name'], app['did'], new_app.app_id))
            self.feedback_application(new_app)
        else:
            self.logs.critical('Driver %d does not exist.' % (app['did']))

    # message dispensor
    def dispensor(self, msg):
        if msg['type'] == 'check_worker_TO':
            self.check_workers_heartbeat()
        # msg from application
        if msg['type'] == 'register_app':
            self.register_application(msg['value'])

    # main body
    def run(self):
        # establish listener
        self.listener = SparkConn('localhost', self.config['master_port'])
        
        # set up periodical signal
        timer = threading.Timer(2.0, self.periodical_signal)
        timer.start()

        # main loop
        while True:
            msg = self.listener.accept()
            self.dispensor(json.loads(msg)['value'])


# instantiation
app = Application()
app.run()
