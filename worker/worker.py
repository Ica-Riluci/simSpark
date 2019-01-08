import logging
import logging.handlers
import threading
# from daemon import runner
from datetime import timedelta, datetime
import sys
import time
import executor

sys.path.append('..')

from publib.SparkConn import *

class appInfo:
    def __init__(self, id, host, port, worker):
        self.id = id
        self.context = executor.sparkContext(id, worker, host, port)

class workerBody:

    def __init__(self):
        # initialize logger
        self.logs = logging.getLogger('simSparkLog')
        self.logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark_worker.log', maxBytes=10000000, backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logs.addHandler(fh)

        self.logs.info('simSpark worker has been awaken.')
        self.config = self.load_config()
        self.executors = []
        self.executors_status = []
        self.exeid = -1
        self.connected = False
        self.id = -1
        self.appId = -1
        self.maxExectuorNum = 10
        self.fetchLock = None
        self.listener = None
        self.driver_listener = None

        self.appList = []

    # functions of Spark Context
    def getRdd(self, index, host, port):
        for e in self.RDDList:
            if e.id == index:
                return e
        rdd = self.fetch_info(index, host, port)
        self.RDDList.append(rdd)
        return rdd

    def getPartition(self, pid, rdd, host, port):
        for p in self.partitionList:
            if p['pid'] == pid & p['rddid'] == rdd['id']:
                return p.data
        if rdd['dependencies'] == []:
            data = self.fetch_data(pid, rdd['id'], host, port)
            patitionRes = {
                'pid': pid,
                'rddid': rdd['id'],
                'data': data
            }
            self.setPartition(pid, rdd, data)
            self.partitionList.append(patitionRes)
            return data
        return None

    def setPartition(self, pid, rddid, data):
        isIn = False
        for p in self.partitionList:
            if p['pid'] == pid & p['rddid'] == rddid:
                p['data'] = data
                isIn = True
        if not(isIn):
            np = {
                'pid': pid,
                'rddid': rddid,
                'data': data
            }
            self.partitionList.append(np)

    # todo give a lock to this function
    def fetch_info(self, rddid, host, port):
        self.fetchLock.acquire()
        msg = {
            'rid': rddid,
            'host': self.config['worker_host'],
            'port': self.config['fetch_port'],
        }
        wrapMsg = self.wrap_msg(host, port, 'fetch_info', msg)
        self.driver_listener.sendMessage(wrapMsg)
        msg = None
        while True:
            msg = self.driver_listener.accept()
            if msg['type'] == 'fetch_info_ack':
                self.fetchLock.release()
                return msg['value']

    # todo give a lock to this function
    def fetch_data(self, pid, rddid, host, port):
        self.fetchLock.acquire()
        msg = {
            'pidx': pid,
            'rid': rddid,
            'host': self.config['worker_host'],
            'port': self.config['fetch_port'],
        }
        wrapMsg = self.wrap_msg(host, port, 'fetch_data', msg)
        self.driver_listener.sendMessage(wrapMsg)
        msg = None
        while True:
            msg = self.driver_listener.accept()
            if msg['type'] == 'fetch_data_ack':
                self.fetchLock.release()
                return msg['value']

    # without changed,need change after use
    def load_config(self):
        config = {
            'master_host': 'localhost',
            'master_port': 7077,
            'worker_host': 'localhost',
            'worker_port': 8801,
            'webui_port': 8080,
            'fetch_port': 9000,
            'worker_timeout': 60000,
            'spread_out': True,
            'default_core': -1,
            'reaper_iteration': 15,
            'executor_max_retries': 10
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
        # initialize the fetch_port lock and the driver_listener
        self.fetchLock = threading.Lock()
        self.driver_listener = SparkConn(self.config['worker_host'], self.config['fetch_port'])

    def reg_succ_executor(self, value):
        oid = value['original']
        e = self.search_executor_by_id(oid)
        if e:
            self.executors[e].id = value['assigned']
            self.executors[e].status = 'ALIVE'
        else:
            self.logs.warning('Failed to read the right executor')

    def send_executor_status(self):
        if self.connected:
            renew_list = []
            exelen = len(self.executors)
            for e in range(0, exelen):
                exe = self.executors[e]
                if exe.status != self.executors_status[e].status:
                    renew_list.append({
                        'id': exe.id,
                        'status': exe.status
                    })
                    self.executors_status[e].status = exe.status
            if not(renew_list == []):
                msg = {
                    'id': self.id,
                    'host': self.config['worker_host'],
                    'port': self.config['worker_port'],
                    'list': renew_list
                }
                wrappedmsg = self.wrap_msg(self.config['master_host'], self.config['master_port'], 'update_executors', msg)
                self.listener.sendMessage(wrappedmsg)
            # check if there is an executor is completed
            eid_list = []
            for nex in renew_list:
                if nex.status == 'COMPLETED':
                    eid_list.append(nex['id'])
            if not(eid_list == []):
                delmsg = {
                    'host': self.config['worker_host'],
                    'port': self.config['worker_port'],
                    'eid': eid_list
                }
                wrapmsg = self.wrap_msg(self.config['master_host'], self.config['master_port'], 'kill_executor', delmsg)
                self.listener.sendMessage(wrapmsg)
        status_renew_timer = threading.Timer(2.0, self.send_executor_status)
        status_renew_timer.start()

    # todo
    def del_executor(self, value):
        if value['success']:
            id = value['eid']
            pos = self.search_executor_by_id(id)
            del self.executors[pos]
            del self.executors_status[pos]

    # todo
    def req_executor(self, value):
        num = value['number']
        host = value['host']
        port = value['port']
        # self.appId = value['app_id']
        elist = []
        for i in range(1, num):
            ex = executor.executor(self.exeid, value['app_id'], self, host, port)
            self.executors.append(ex)
            self.executors_status.append(ex.status)
            idmsg = {
                'id': self.exeid,
                'app_id': value['app_id']
            }
            elist.append(idmsg)
            msg = {
                'id': self.id,
                'list': elist
            }
            wrapmsg = self.wrap_msg(self.config['master_host'], self.config['master_port'], 'update_executors', msg)
            self.exeid -= 1

    def send_heartbeat(self):
        if self.connected:
            msg = {
                    'id': self.id,
                    'host': self.config['worker_host'],
                    'port': self.config['worker_port'],
                    'time': datetime.now()
                }
            wrapmsg = self.wrap_msg(self.master.host, self.master.port, 'worker_heartbeat', msg)
            self.listener.sendMessage(wrapmsg)
        pass

    '''
    def cleanCatalog(self):
        pass
    '''

    def register_worker(self):
        worker = {
            'host': self.config['worker_host'],
            'port': self.config['worker_port']
        }
        wrapped_msg = self.wrap_msg(self.config['master_host'], self.config['master_port'], 'register_worker', worker)
        # print wrapped_msg
        self.listener.sendMessage(wrapped_msg)
        if self.connected == False:
            reg_timer = threading.Timer(5.0, self.register_worker)
            reg_timer.start()

    def ghost_executor(self, value):
        pass

    def reregister(self):
        self.connected = False
        self.register_worker()

    # todo open the thread pool to run the executors in parallel, still need to add port and host
    def pending_task(self, value):
        eid = value['eid']
        rid = value['rid']
        pid = value['pidx']
        appid = value['appid']
        host = value['host']
        port = value['port']
        app = self.search_app_by_id(appid, host, port)
        index = self.search_executor_by_id(eid)
        if index != None:
            self.executors[index].setId(rid, pid)
            self.executors[index].start()
        else:
            self.logs.critical('Missing executor id.')

    # wrap the message
    def wrap_msg(self, address, port, type, value):
        raw = {
            'type': type,
            'value': value
        }
        wrapped = {
            'host': address,
            'port': port,
            'value': json.dumps(raw)
        }
        return wrapped

    def search_executor_by_id(self, id):
        for e in self.executors:
            if e.executor_id == id:
                return self.executors.index(e)
        return None

    def search_app_by_id(self, id, host, port):
        for e in self.appList:
            if e.id == id:
                return self.appList.index(e)
        app = appInfo(id, host, port, self)
        self.appList.append(app)
        return app

    def process(self, msg):
        if msg['type'] == 'request_resource':
            self.req_executor(msg['value'])
        elif msg['type'] == 'register_worker':
            self.reregister()
        elif msg['type'] == 'register_executor_success':
            self.reg_succ_executor(msg['value'])
        elif msg['type'] == 'elimination_feedback':
            self.del_executor(msg['value'])
        elif msg['type'] == 'ghost_executor':
            self.ghost_executor(msg['value'])
        elif msg['type'] == 'pending_task':
            self.pending_task(msg['value'])

    def run(self):
        self.listener = SparkConn(self.config['worker_host'], self.config['worker_port'])

        # a timer to set initial register
        reg_timer = threading.Timer(5.0, self.register_worker)
        reg_timer.start()
        while True:
            msg = self.listener.accept()
            if msg['type'] == 'register_worker_success':
                self.reg_succ_worker(msg['value'])
                break

        # a timer to send the status change within a period of time
        status_renew_timer = threading.Timer(2.0, self.send_executor_status)
        status_renew_timer.start()
        # onStart

        while True:
            msg = self.listener.accept()
            # print str(msg)
            # print str(msg['value'])
            self.process(msg)

app = workerBody()
app.run()

