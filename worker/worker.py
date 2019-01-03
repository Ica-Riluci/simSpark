import logging
import logging.handlers
import threading
# from daemon import runner
from datetime import timedelta, datetime
import sys
import time
import executor

sys.path.append('publib')

from publib.SparkConn import *

# without changed,need change after use
def load_config(logs):
    config = {
        'master_address': '',
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

class workerBody:

    def __init__(self):
        # initialize logger
        self.logs = logging.getLogger('simSparkLog')
        self.logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark_master.log', maxBytes=10000000, backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logs.addHandler(fh)

        self.logs.info('simSpark worker has been awaken.')
        self.config = self.load_config()
        self.executors = []

    # load configuration
    def load_config(self):
        self.logs.info('<worker_config.json> is about to be loaded.')
        config = {
            'worker_port': 7077,
        }
        try:
            with open('worker_config.json', 'r') as jsoninput:
                inp = json.load(jsoninput)
            for k in config.keys():
                if k in inp.keys():
                    config[k] = inp[k]
        except IOError:
            self.logs.warning('Failed to read configuration. Use default instead.')
        return config

    # todo:need the worker to send a host and port
    def reg_succ_worker(self, value):
        self.connected = True
        self.id = value['id']

    # todo begin the thread of the executor
    def reg_succ_executor(self, value):
        oid = value['original']
        e = self.search_executor_by_id(oid)
        if e:
            self.executors[e].id = value['assigned']
            self.executors[e].status = 'alive'
        else:
            self.logs.warning('Failed to read the right executor')

    def send_executor_status(self):
        if self.connected:
            renew_list = []
            for ex in self.executors:
                if ex.main.status != ex.status:
                    renew_list.append(ex.main)
                    ex.status = ex.main.status
            if not(renew_list) == False:
                msg = {
                    'id': self.id,
                    'host': self.config['host'],
                    'port': self.config['port'],
                    'list': self.renew_list
                }
                wrappedmsg = self.wrap_msg(self.config.master_host, self.config.master_port, 'update_executors', msg)
                self.listener.sendMessage(wrappedmsg)
            # check if there is an executor is completed
            eid_list = []
            for nex in renew_list:
                if nex.main.status == 'Completed':
                    eid_list.append(nex.eid)
            delmsg = {
                    'host': self.config['host'],
                    'port': self.config['port'],
                    'eid': self.eid_list
                }
            wrapmsg = self.wrap_msg(self.config.master_host, self.config.master_port, 'kill_executor', delmsg)
            self.listener.sendMessage(wrapmsg)
    '''
    def exec_state_change(self, value):
        # change the data itself
        change_state = {
            'id': value['id'],
            'status': value['status'],
            'app_id': value['app_id']
        }
        eid = value['id']
        pos = -1
        for e in self.renew_exe_list:
            if e['id'] == eid:
                pos = self.renew_exe_list.index(e)
        if pos != -1:
            self.renew_exe_list[pos] = change_state
        else:
            self.renew_exe_list.append(change_state)
    '''

    # todo
    def del_executor(self, value):
        if value['success']:
            id = value['eid']
            pos = self.search_executor_by_id(id)
            del self.executors[pos]

    # todo
    def req_executor(self, value):
        num = value['number']
        self.appId = value['app_id']
        for i in range(1, num):
            ex = executor(self.exeid)
            self.exeid -= 1
            self.executors.append(ex)
            self

    def send_heartbeat(self):
        if self.connected:
            msg = {
                    'id': self.id,
                    'host': self.config['host'],
                    'port': self.config['port'],
                    'time': datetime.now()
                }
            wrapmsg = self.wrap_msg(self.master.host, self.master.port, 'worker_heartbeat', msg)
            self.listener.sendMessage(wrapmsg)
        pass

    def cleanCatalog(self):
        pass

    def register_worker(self):
        worker = {
            'host': self.config.host,
            'port': self.config.port
        }
        wrapped_msg = self.wrap_msg(self.config.master_host, self.config.master_port, 'register_worker', worker)
        self.self.listener.sendMessage(wrapped_msg)

    # todo
    def reregister(self):
        self.conected = False
        self.register_worker()

    # wrap the message
    def wrap_msg(self, address, port, type, value):
        raw = {
            'type': type,
            'value': value
        }
        wrapped = {
            'host': address,
            'port': port,
            'value': raw
        }
        return wrapped

    def search_executor_by_id(self, id):
        for e in self.executors:
            if e.executor_id == id:
                return self.executors.index(e)
        return None

    def process(self, msg):
        if msg['type'] == 'register_worker_success':
            self.reg_succ_worker(msg['value'])
        elif msg['type'] == 'request_resource':
            self.req_executor(msg['value'])
        elif msg['type'] == 'register_worker':
            self.reregister()
        elif msg['type'] == 'register_executor_success':
            self.reg_succ_executor(msg['value'])
        elif msg['type'] == 'elimination_feedback':
            self.del_executor(msg['value'])
        elif msg['type'] == 'ghost_executor':
            self.ghost_executor(msg['value'])

    def run(self):
        self.logs = logging.getLogger('simSparkLog')
        self.logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark.log', maxBytes=10000000, backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logs.addHandler(fh)

        self.config = load_config(self.logs)
        '''
        if self.config['default_core'] < 1 and self.config['default_core'] != -1:
            self.logs.critical('Default core(s) assigned must be positive.')
            return
        '''
        # fetch configuration
        self.exeid = -1
        self.connected = False
        self.executors = []
        self.id = -1
        self.appId = -1
        # self.renew_exe_list = []
        self.maxExectuorNum = 10

        # recording structure

        while self.connected == False:
            self.register_worker()
            time.sleep(3)

        # todo set a thread pool

        # a timer to send the status change within a period of time
        status_renew_timer = threading.Timer(1.0, self.send_executor_status)
        status_renew_timer.start()

        self.listener = SparkConn('localhost', self.config['worker_port'])

        # onStart

        while True:
            msg = self.listener.accept()
            self.process(json.loads(msg['value']))

app = workerBody()
app.run()

