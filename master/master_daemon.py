import logging
import logging.handlers
import threading
import time
from datetime import timedelta, datetime
import json
from daemon import runner
import sys

sys.path.append('publib')

from publib.SparkConn import *

class SparkUnit:
    spark_unit_count = 0
    def __init__(self, add, po):
        self.address = add
        self.port = po
        self.lasthb = datetime.now()
        self.alive = True
        SparkUnit.spark_unit_count += 1
# this class denotes all kind of units managed by master:
# application, worker, driver

class WorkerUnit(SparkUnit):
    worker_count = 0
    def __init__(self, add, po, cores, ram):
        WorkerUnit.worker_count += 1
        super(WorkerUnit, self).__init__(add, po)
        self.worker_id = WorkerUnit.worker_count
        self.used_cores = cores
        self.all_ram = ram

class DriverUnit(SparkUnit):
    driver_count = 0
    def __init__(self, add, po):
        DriverUnit.driver_count += 1
        super(DriverUnit, self).__init__(add, po)
        self.driver_id = DriverUnit.driver_count
        self.state = 'IDLE'

class ExecutorUnit(SparkUnit):
    executor_count = 0
    def __init__(self, add, po):
        ExecutorUnit.executor_count += 1
        super(ExecutorUnit, self).__init__(add, po)
        # address and port are information of its worker
        self.exec_id = ExecutorUnit.executor_count
        self.state = 'IDLE'

class ApplicationUnit(SparkUnit):
    app_count = 0
    def __init__(self, add, po, app):
        ApplicationUnit.app_count += 1
        super(ApplicationUnit, self).__init__(add, po)
        self.app_name = app['name']
        self.app_id = ApplicationUnit.app_count
        self.driver_id = app['driver_id']
        self.state = 'SLEEP'
        self.exec_list = []

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

    def reg_worker(self, rw):
        foundins = False
        for w in self.workers:
            if w.address == rw['host']:
                foundins = True
                if w.alive:
                    self.logs.error('The worker %s is already registered and alive.' % (w.address))
                    msg = {
                        'type' : 'register_worker_failed',
                        'value' : ''
                    }
                    wrappedmsg = {
                        'host' : w.address,
                        'port' : w.port,
                        'value' : msg
                    }
                    self.listener.sendMessage(wrappedmsg)
                else:
                    self.logs.info('The worker %s is registered but not alive.' % (w.address))
                    self.logs.info('Trial to wake up the worker %s.' % (w.address))
                    self.workers[self.workers.index(w)].alive = True
                    self.workers[self.workers.index(w)].used_cores = rw['cores']
                    self.workers[self.workers.index(w)].all_ram = rw['ram']
                    self.workers[self.workers.index(w)].port = rw['port']
                    self.workers[self.workers.index(w)].lasthb = datetime.now()
                    break
        if not foundins:
            self.logs.info('The worker %s is new to the master.' % (rw.address))
            new_workers = WorkerUnit(rw['host'], rw['port'], rw['cores'], rw['ram'])
            self.workers.append(new_workers)

    def reg_app(self, ra):
        self.logs.info('Prepare to register application %s.' % (ra['app']['name']))
        new_app = ApplicationUnit(ra['driver']['host'], ra['driver']['port'], ra['app'])
        self.apps.append(new_app)
        msg = {
            'type' : 'reg_app_ack',
            'value' : new_app.app_id
        }
        wrappedmsg = {
            'host' : new_app.address,
            'port' : new_app.port,
            'value' : msg
        }
        self.listener.sendMessage(wrappedmsg)
        # self.schedule()

    def send_check_worker_timeout(self):
        msg = {
          'type' : 'check_worker_timeout',
          'value' : ''
        }
        wrappedmsg = {
          'host' : 'localhost',
          'port' : self.config['master_port'],
          'value' : msg
        }
        self.listener.sendMessage(wrappedmsg)
        # send msg

        timer = threading.Timer(2.0, self.send_check_worker_timeout)
        timer.start()
        # restart timer

    def check_worker_timeout(self):
        for w in self.workers:
            if w.alive == True:
                if datetime.now() - w.lasthb > timedelta(microseconds=self.config['worker_timeout']):
                    self.workers[self.workers.index(w)].alive = False
                    self.logs.warning('Set worker %s to dead because the last heartbeat received is earlier than %d ms ago.' % (w.address, self.config['worker_time_out']))
            else:
                if datetime.now() - w.lasthb > timedelta(microseconds=(self.config['reaper_iteration'] + 1) * self.config['worker_timeout']):
                    self.workers.remove(w)
                    self.logs.warning('Bury worker %s because its heartbeat has been undetected for severl iterations.' % (w.address))

    def search_app_by_id(self, id):
        for i in range(0, len(self.apps)):
            if self.apps[i].app_id == id:
                return i
        return -1

    def search_exec_by_id(self, id):
        for i in range(0, len(self.executors)):
            if self.executors[i].exec_id == id:
                return i
        return -1
    
    def exec_stage_changed_ack(self, esc):
        app_idx = self.search_app_by_id(esc['app_id'])
        exec_idx = self.search_exec_by_id(esc['exec_id'])
        if app_idx == -1 or exec_idx == -1:
            self.logs.error('Application %d or executor %d not found.' % (esc['app_id'], esc['exec_id']))
            return
        if esc['exec_id'] not in self.apps[app_idx].exec_list:
            self.logs.error('Executor %d is unknown to application %d.' % (esc['exec_id'], esc['app_id']))
            return
        old_state = self.executors[exec_idx].state
        self.executors[exec_idx].state = esc['state']
        if esc['state'] == 'RUNNING' and old_state != 'LANCHING':
            self.logs.warning('Illegal state change for executor %d' % (esc['exec_id']))
        msg = {
            'type' : 'executor_update',
            'value' : {
                'id' : exec_idx,
                'state' : esc['state']
            }
        }
        wrapped_msg = {
            'host' : self.apps[app_idx].address,
            'port' : self.apps[app_idx].port,
            'value' : msg
        }
        self.listener.sendMessage(wrapped_msg)
        if esc['state'] == 'FINNISHED':
            self.logs.info('Remove executor %d because it is %s state' % (esc['exec_id'], esc['state']))
            if self.apps[app_idx].state != 'FINISHED':
                self.apps[app_idx].exec_list.remove(esc['exec_id'])
            self.executors.remove(self.executors[exec_idx])
        # self.schedule()
        # work to be done

    def rm_app(self, app):
        app_idx = self.search_app_by_id(app['id'])
        if app_idx == -1:
            self.logs.error('Application %d does not exist.' % (app['id']))
            return
        else:
            executors_list = self.apps[app_idx].exec_list
            for exec_id in executors_list:
                exec_idx = self.search_exec_by_id(exec_id)
                if exec_idx == -1:
                    self.logs.error('Executor %d does not exist.' % (exec_id))
                else:
                    msg = {
                        'type' : 'kill_executor',
                        'value' : exec_id
                    }
                    warpped_msg = {
                        'host' : self.executors[exec_idx].address,
                        'port' : self.executors[exec_idx].port,
                        'value' : msg
                    }
                    self.listener.sendMessage(warpped_msg)
                    self.executors.remove(self.executors[exec_idx])
                # self.schedule()
            self.apps.remove(self.apps[app_idx])
    
    def search_driver_by_id(self, id):
        for i in range(0, len(self.drivers)):
            if self.drivers[i].driver_id == id:
                return i
        return -1

    def driver_state_changed_ack(self, driver):
        driver_idx = self.search_driver_by_id(driver['id'])
        if driver_idx == -1:
            self.logs.error('Driver %d not found.' % (driver['id']))
            return
        else:
            if self.drivers[driver_idx] in ['ERROR', 'FINISHED', 'KILLED', 'FAILED']:
                self.drivers.remove(self.drivers[driver_idx])
                # self.schedule
            else:
                self.logs.error('Unexpected update for driver %d.' % (driver['id']))

    def process(self, msg):
        if msg['type'] == 'check_worker_timeout':
            self.check_worker_timeout()
        elif msg['type'] == 'reg_worker':
            self.reg_worker(msg['value'])
        elif msg['type'] == 'reg_app':
            self.reg_app(msg['value'])
        elif msg['type'] == 'exec_stage_changed':
            self.exec_stage_changed_ack(msg['value'])
        elif msg['type'] == 'rm_app':
            self.rm_app(msg['value'])
        elif msg['type'] == 'driver_state_changed':
            self.driver_state_changed_ack(msg['value'])
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
        self.logs = logging.getLogger('simSparkLog')
        self.logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark.log',maxBytes=10000000,backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logs.addHandler(fh)

        self.config = load_config(self.logs)
        if self.config['default_core'] < 1 and self.config['default_core'] != -1:
            self.logs.critical('Default core(s) assigned must be positive.')
            return
        # fetch configuration

        # app_id = []
        # app_wl = []
        self.apps = []
        # app_ad = []
        # app_completed = []
        # app_next = 0

        self.workers = []
        # worker_ad = []
        
        # drivers = []
        self.drivers = []
        self.executors = []
        # driver_completed = []
        # driver_wl = []
        # recording structure

        timer = threading.Timer(2.0, self.send_check_worker_timeout)
        timer.start()

        self.listener = SparkConn('localhost', self.config['master_port'])

        # onStart

        while True:
            msg = self.listener.accept()
            self.process(json.loads(msg['value']))
            pass
        # listening - this part should contains how the daemon listens from socket $master_socket and the result

app = MasterDaemon()
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()