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

    def register_driver_success(self, driver):
        value = {
            'id' : driver.driver_id
        }
        self.listener.sendMessage(self.wrap_msg(
            driver.host,
            driver.port,
            'register_driver_success',
            value
        ))

    def feedback_application(self, app):
        value = {
            'id' : app.id,
            'executor_list' : app.executor_list
        }
        self.listener.sendMessage(self.wrap_msg(app.host, app.port, 'resource_update', value))

    def feedback_worker(self, worker):
        value = worker.worker_id
        self.listener.sendMessage(self.wrap_msg(worker.host, worker.port, 'register_worker_success', value))

    def awake_ghost_worker(self, ghost_heartbeat):
        self.listener.sendMessage(self.wrap_msg(
            ghost_heartbeat['host'],
            ghost_heartbeat['port'],
            'register_worker',
            None
        ))

    def feedback_executor(self, executor, oid):
        value = {
            'original' : oid,
            'assigned' : executor.executor_id
        }
        self.listener.sendMessage(self.wrap_msg(
            executor.host,
            executor.port,
            'register_executor_success',
            value
        ))

    def inform_application_ready(self, app):
        value = []
        for e in app.executor_list:
            e_idx = self.search_executor_by_id(e)
            value.append({
                'executor_id' : e,
                'host' : self.executors[e_idx].host,
                'port' : self.executors[e_idx].port
            })
        self.listener.sendMessage(self.wrap_msg(
            app.host,
            app.port,
            'resource_ready',
            value
        ))

    def feedback_ghost_executor(self, host, port, eid):
        self.listener.sendMessage(self.wrap_msg(
            host,
            port,
            'ghost_executor',
            eid
        ))
    
    def feedback_executor_elimination(self, exec, e_idx):
        value = {
            'eid' : exec['eid'],
            'success' : not not e_idx
        }
        self.listener.sendMessage(self.wrap_msg(
            exec['host'],
            exec['port'],
            'elimination_feedback',
            value
        ))

    def inform_no_resource(self, driver):
        self.listener.sendMessage(self.wrap_msg(
            driver.host,
            driver.port,
            'no_resource',
            None
        ))

    def request_resource(self, wid, num, aid):
        w_idx = self.search_worker_by_id(wid)
        value = {
            'number' : num,
            'app_id' : aid
        }
        self.listener.sendMessage(self.wrap_msg(
            self.workers[w_idx].host,
            self.workers[w_idx].port,
            'request_resource',
            value
        ))

    def inform_wait_allocation(self, driver):
        self.listener.sendMessage(self.wrap_msg(
            driver.host,
            driver.port,
            'wait_allocation',
            None
        ))
    
    def inform_app_still_running(self, driver):
        self.listener.sendMessage(self.wrap_msg(
            driver.host,
            driver.port,
            'app_still_running',
            None
        ))

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
    
    def search_application_by_id(self, id):
        for a in self.apps:
            if a.app_id == id:
                return self.apps.index(a)
        return None

    def search_executor_by_id(self, id):
        for e in self.executors:
            if e.executor_id == id:
                return self.executors.index(e)
        return None

    def search_worker_by_id(self, id):
        for w in self.workers:
            if w.worker_id == id:
                return self.workers.index(w)
        return None

    def search_worker_by_address(self, address):
        for w in self.workers:
            if w.host == address:
                return self.workers.index(w)
        return None

    def kill_executors(self, eliminate_list):
        for e in eliminate_list:
            e_idx = self.search_executor_by_id(e)
            if e_idx:
                worker_idx = self.search_worker_by_id(self.executors[e_idx].worker_id)
                app_idx = self.search_application_by_id(self.executors[e_idx].app_id)
                if worker_idx:
                    if e in self.workers[worker_idx].executor_list:
                        if self.workers[worker_idx].alive:
                            self.logs.warning('The worker %d supervising the executor %d is still alive.' % (self.executors[e_idx].worker_id, e))
                        self.workers[worker_idx].executor_list.remove(e)
                    else:
                        self.logs.warning('The worker %d does not supervise executor %d.' % (self.executors[e_idx].worker_id, e))
                else:
                    self.logs.warning('The worker %d does not exist.' % (self.executors[e_idx].worker_id))
                if app_idx:
                    if e in self.apps[app_idx].executor_list:
                        if self.apps[app_idx].status != 'ELIMINATED':
                            self.logs.warning('The application %d using the executor %d is still alive.' % (self.executors[e_idx].app_id, e))
                        self.apps[app_idx].executor_list.remove(e)
                    else:
                        self.logs.warning('The application %d does not use executor %d.' % (self.executors[e_idx].app_id, e))
                else:
                    self.logs.warning('The application %d does not exist.' % (self.executors[e_idx].app_id))
                self.executors.remove(self.executors[e_idx])
            else:
                self.logs.warning('The executor %d does not exist.' % (e))

    def check_application_ready(self, aid):
        app_idx = self.search_application_by_id(aid)
        if app_idx:
            if self.apps[app_idx].status == 'WAIT':
                if len(self.apps[app_idx].executor_list) >= self.apps[app_idx].executors_req:
                    self.inform_application_ready(self.apps[app_idx])
        else:
            self.logs.error('Application %d does not exist.' % (aid))

    def register_executor(self, address, port, wid, eid, aid):
        worker_idx = self.search_worker_by_id(wid)
        app_idx = self.search_application_by_id(aid)
        if worker_idx:
            if app_idx:
                self.logs.info('New executor for application %d on worker %d is registered' % (aid, wid))
                new_executor = ExecutorUnit(address, port, wid, aid)
                self.workers[worker_idx].executor_list.append(new_executor)
                self.apps[app_idx].executor_list.append(new_executor)
                self.feedback_executor(new_executor, eid)
                self.check_application_ready(aid)
            else:
                self.logs.error('Application %d does not exist.' % (aid))
        else:
            self.logs.error('Worker %d does not exists.' % (wid))


    # reaction to message
    def check_workers_heartbeat(self):
        for worker in self.workers:
            if worker.alive:
                if worker.heartbeat_expired(self.config['worker_timeout']):
                    self.logs.warning('Worker %d is out of contact.' % (worker.id))
                    worker.alive = False
            else:
                if worker.dead(self.config['worker_timeout'], self.config['reap_iteration']):
                    self.logs.warning('Worker %d will be buried for out of contact after several iterations.' % (worker.id))
                    self.kill_executors(worker.executor_list)
                    self.workers.remove(worker)

    def register_application(self, app):
        self.logs.info('Request for registration of application %s received.' % (app['name']))
        driver_idx = self.search_driver_by_id(app['did'])
        if driver_idx:
            if self.drivers[driver_idx].app_id:
                self.logs.critical('An application is already binded to driver %d.' % (app['did']))
                return
            new_app = ApplicationUnit(app['host'], app['port'], app['name'], app['did'])
            self.apps.append(new_app)
            self.logs.info('Application %s is binded to driver %d using id %d.' % (app['name'], app['did'], new_app.app_id))
            self.feedback_application(new_app)
        else:
            self.logs.critical('Driver %d does not exist.' % (app['did']))

    def kill_application(self, app):
        app_idx = self.search_application_by_id(app['id'])
        if app_idx:
            self.apps[app_idx].status = 'ELIMINATED'
            if len(self.apps[app_idx].executor_list) > 0:
                self.logs.warning('There are executors obtained by application %d.' % (app['id']))
                self.kill_executors(self.apps[app_idx].executor_list)
            driver_idx = self.search_driver_by_id(app['driver_id'])
            if driver_idx:
                if self.drivers[driver_idx].app_id != app['id']:
                    self.logs.warning('Driver information not matched.')
                else:
                    self.drivers[driver_idx].set_app_id()
            else:
                self.logs.warning('None of the drivers is binded with application %d.' % (app['id']))
            self.apps.remove(self.apps[app_idx])
        else:
            self.logs.warning('Application %d does not exist.' % (app['id']))

    def worker_heartbeat_ack(self, heartbeat):
        worker_idx = self.search_worker_by_id(heartbeat['id'])
        if worker_idx:
            if self.workers[worker_idx].host == heartbeat['host']:
                if not self.workers[worker_idx].alive:
                    self.logs.info('Worker %d is awaken.' % (heartbeat['id']))
                    self.workers[worker_idx].awake()
                self.workers[worker_idx].update_heartbeat(heartbeat['time'])
            else:
                self.logs.error('Worker %d information does not match with the latest heartbeat.' % (heartbeat['id']))
        else:
            self.logs.warning('Ghost worker {%s} revives.' % (heartbeat['host']))
            self.awake_ghost_worker(heartbeat)            

    def register_worker(self, worker):
        worker_idx = self.search_worker_by_address(worker['host'])
        if worker_idx:
            self.logs.critical('Worker {%s} already exists.' % worker['host'])
            return
        else:
            new_worker = WorkerUnit(worker['host'], worker['port'])
            self.logs.info('Worker {%s} registers as worker %d.' % (worker['host'], new_worker.worker_id))
            self.workers.append(new_worker)
            self.feedback_worker(new_worker)

    def update_executors_of_worker(self, worker):
        worker_idx = self.search_worker_by_id(worker['id'])
        if worker_idx:
            for exec in worker['list']:
                if exec['id'] < 0:
                    self.register_executor(worker['host'], worker['port'], worker['id'], exec['id'], exec['app_id'])
                else:
                    e_idx = self.search_executor_by_id(exec['id'])
                    if e_idx:
                        self.executors[e_idx].status = exec['status']
                    else:
                        self.logs.error('Executor %d does not exist.' % (exec['id']))
                        self.feedback_ghost_executor(worker['host'], worker['port'], exec['id'])
        else:
            self.logs.error('Worker %d does not exists.' % (worker['id']))

    def eliminate_executor(self, exec):
        self.kill_executors([exec['eid']])
        e_idx = self.search_executor_by_id(exec['eid'])
        self.feedback_executor_elimination(exec, e_idx)

    def register_driver(self, driver):
        new_driver = DriverUnit(driver['host'], driver['port'])
        self.drivers.append(new_driver)
        self.register_driver_success(new_driver)
        
    def allocate_resource(self, req):
        d_idx = self.search_driver_by_id(req['driver_id'])
        if not d_idx:
            self.logs.error('Unknown driver requests resource.')
            return
        if not self.drivers[d_idx].app_id:
            self.logs.error('Driver %d which no applicaiton is binded to requests resource.' % req['driver_id'])
            return
        if not req['number'] > 0:
            self.logs.warning('Empty request from driver %d.' % (req['driver_id']))
            return
        a_idx = self.search_application_by_id(self.drivers[d_idx].app_id)
        self.apps[a_idx].executors_req = req['number']
        asstable = {}
        class WorkerHeap():
            def __init__(self):
                self.heap = [{}]
            
            def pop(self, i):
                if i == 1:
                    return
                if self.heap[i]['weight'] < self.heap[i // 2]['weight']:
                    tmp = self.heap[i // 2]
                    self.heap[i // 2] = self.heap[i]
                    self.heap[i] = tmp
                    self.pop(i // 2)

            def sink(self, i):
                if i * 2 >= len(self.heap):
                    return
                if self.heap[i]['weight'] > self.heap[i * 2]['weight']:
                    sink_left = True
                    if i * 2 + 1 < len(self.heap):
                        if self.heap[i * 2]['weight'] > self.heap[i * 2 + 1]['weight']:
                            sink_left = False
                            tmp = self.heap[i]
                            self.heap[i] = self.heap[i * 2 + 1]
                            self.heap[i * 2 + 1] = tmp
                            self.sink(i * 2 + 1)
                    if sink_left:
                        tmp = self.heap[i]
                        self.heap[i] = self.heap[i * 2]
                        self.heap[i * 2] = tmp
                        self.sink(i * 2)
                else:
                    if i * 2 + 1 < len(self.heap):
                        if self.heap[i * 2 + 1]['weight'] < self.heap[i]['weight']:
                            tmp = self.heap[i]
                            self.heap[i] = self.heap[i * 2 + 1]
                            self.heap[i * 2 + 1] = tmp
                            self.sink(i * 2 + 1)

            def insert(self, id, payload):
                node = {
                    'id' : str(id),
                    'weight' : payload
                }
                self.heap.append(node)
                self.pop(len(self.heap) - 1)

            def add_payload(self):
                if len(self.heap) < 2:
                    return
                self.heap[1]['weight'] + 1
                self.sink(1)
        
        payload_heap = WorkerHeap()

        for w in self.workers:
            asstable[str(w.worker_id)] = 0
            payload_heap.insert(
                w.worker_id,
                len(w.executor_list)
            )
        if len(payload_heap.heap) < 2:
            self.logs.error('No resource can be allocated.')
            self.inform_no_resource(self.drivers[d_idx])
            return
        for i in range(0, req['number']):
            asstable[payload_heap.heap[1]['id']] += 1
            payload_heap.add_payload()
        for k in asstable.keys():
            self.request_resource(int(k), asstable[k], self.drivers[d_idx].app_id)
        self.inform_wait_allocation(self.drivers[d_idx])

    def kill_driver(self, did):
        d_idx = self.search_driver_by_id(did)
        if d_idx:
            if self.drivers[d_idx].app_id:
                self.logs.error('Application %d of driver %d is still running.' % (self.drivers[d_idx].app_id, did))
                self.inform_app_still_running(self.drivers[d_idx])
                return
            self.logs.error('Driver %d is killed.' % (did))
            self.drivers.remove(self.drivers[d_idx])
        else:
            self.logs.error('Driver %d does not exist.' % (did))
        
    # message dispensor
    def dispensor(self, msg):
        if msg['type'] == 'check_worker_TO':
            self.check_workers_heartbeat()
        # msg from application
        elif msg['type'] == 'register_app':
            self.register_application(msg['value'])
        elif msg['type'] == 'kill_app':
            self.kill_application(msg['value'])
        # msg from worker
        elif msg['type'] == 'worker_heartbeat':
            self.worker_heartbeat_ack(msg['value'])
        elif msg['type'] == 'register_worker':
            self.register_worker(msg['value'])
        elif msg['type'] == 'update_executors':
            self.update_executors_of_worker(msg['value'])
        elif msg['type'] == 'kill_executor':
            self.eliminate_executor(msg['value'])
        # msg from driver
        elif msg['type'] == 'register_driver':
            self.register_driver(msg['value'])
        elif msg['type'] == 'request_resource':
            self.allocate_resource(msg['value'])
        elif msg['type'] == 'kill_driver':
            self.kill_driver(msg['value'])

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