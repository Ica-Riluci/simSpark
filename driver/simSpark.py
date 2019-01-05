'''
    This is a library for users to import in order to
    create a simSpark context which allow users defin-
    ing their applications and posting applications t-
    o a certain cluster.
'''
import sys
import json
import threading
import time
import logging
from logging import handlers
sys.path.append('..')
from publib.SparkConn import *

class simApp:
    def __init__(self, name='asimApp'):
        self.app_name = name
        self.app_id = None
        self.status = 'WAIT'
        self.idle_executors = []
        self.busy_executors = []

class simContext:
    def __init__(self, app):
        self.logs = logging.getLogger('simSparkLog')
        self.logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark_driver.log',maxBytes=10000000,backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logs.addHandler(fh)

        self.config = self.load_config()

        self.app = app
        self.driver_id = None
        self.port = self.config['driver_port']
        self.rdds = []
        self.undone = []
        self.listener = SparkConn(self.config['driver_host'], self.config['driver_port'])

        # register driver
        value = {
            'host' : self.config['driver_host'],
            'port' : self.port
        }
        self.listener.sendMessage(self.wrap_msg(
            self.config['master_host'],
            self.config['master_port'],
            'register_driver',
            value
        ))
        while True:
            self.logs.info('Waiting for registeration feedback')
            msg = self.listener.accept()
            if msg['type'] == 'register_driver_success':
                self.driver_id = msg['value']['id']
                break
        # register app
        value = {
            'host' : self.config['self_host'],
            'port' : self.port,
            'did' : self.driver_id,
            'app_name' : self.app.app_name
        }
        self.listener.sendMessage(self.wrap_msg(
            self.config['master_host'],
            self.config['master_port'],
            'register_driver',
            value
        ))
        while True:
            self.logs.info('Wait for registeration feedback')
            msg = self.listener.accept()
            if msg['type'] == 'resource update':
                self.app.app_id = msg['value']['id']
                self.app.idle_executor = msg['value']['idle_executor']
                self.app.busy_executor = msg['value']['busy_executor']
                break

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

    def load_config(self):
        self.logs.info('<master_config.json> is about to be loaded.')
        config = {
            'self_host' : 'localhost',
            'master_host' : 'localhost',
            'master_port' : 7077,
            'timeout' : 60
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