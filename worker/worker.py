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
        self.id = -1
        self.appId = -1
        self.maxExectuorNum = 10
        self.fetchLock = None
        self.listener = None
        self.driver_listener = None

        self.appList = []

    def __del__(self):
        global timer
        if timer:
            timer.cancel()

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

    def fetch_data(self, rddid, pid, host, port):
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

    # todo still need to confirm the interface
    def send_result(self, rddid, pid, host, port):
        self.fetchLock.acquire()
        msg = {
            'host': self.config['worker_host'],
            'port': self.config['worker_port'],
            'pidx': pid,
            'rid': rddid,
        }
        wrapMsg = self.wrap_msg(host, port, 'task_finished', msg)
        self.driver_listener.sendMessage(wrapMsg)
        self.fetchLock.release()

    def send_data_to_driver(self, value):
        appid = value['appid']
        rid = value['rid']
        pidx = value['pidx']
        dport = value['driver_port']
        e = self.search_app_by_id(appid)
        ctx = self.appList[e].context
        result = ctx.get_partition_data(rid, pidx)
        wrapmsg = self.wrap_msg(ctx.driverhost, dport, 'fetch_data_ack', result)
        self.listener.sendMessage(wrapmsg)

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
        renew_list = []
        exelen = len(self.executors)
        for e in range(0, exelen):
            exe = self.executors[e]
            if exe.status != self.executors_status[e].status:
                renew_list.append({
                    'id': exe.id,
                    'status': exe.status,
                    'app_id': exe.appid
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
        if not(eid_list == None):
            delmsg = {
                'host': self.config['worker_host'],
                'port': self.config['worker_port'],
                'eid': eid_list
            }
            wrapmsg = self.wrap_msg(self.config['master_host'], self.config['master_port'], 'kill_executor', delmsg)
            self.listener.sendMessage(wrapmsg)
        tick(2.0, self.send_executor_status)

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
        msg = {
                'id': self.id,
                'host': self.config['worker_host'],
                'port': self.config['worker_port'],
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S %f')
            }
        wrapmsg = self.wrap_msg(self.config['master_host'], self.config['master_port'], 'worker_heartbeat', msg)
        self.listener.sendMessage(wrapmsg)

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
        tick(5.0, self.register_worker)

    def ghost_executor(self, value):
        pass

    def reregister(self):
        self.register_worker()

    # todo open the thread pool to run the executors in parallel, still need to add port and host
    def pending_task(self, value):
        eid = value['eid']
        rid = value['rid']
        pid = value['pidx']
        appid = value['appid']
        host = value['host']
        port = value['port']
        app = self.search_app_by_id(appid)
        if not app:
            self.add_app(appid, host, port)
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
        for e in range(0, len(self.executors)):
            if self.executors[e].executor_id == id:
                return e
        return None

    def search_app_by_id(self, id):
        for e in range(0, len(self.appList)):
            if self.appList[e].id == id:
                return e
        return None

    def add_app(self, id, host, port):
        app = appInfo(id, host, port, self)
        self.appList.append(app)

    def delete_app(self, value):
        id = value['appid']
        index = self.search_app_by_id(id)
        if index:
            del self.appList[index]

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
        elif msg['type'] == 'delete_app':
            self.delete_app(msg['value'])
        elif msg['type'] == 'fetch_data':
            self.send_data_to_driver(msg['value'])

    def run(self):
        self.listener = SparkConn(self.config['worker_host'], self.config['worker_port'])

        # a timer to set initial register
        tick(5.0, self.register_worker)
        while True:
            msg = self.listener.accept()
            self.logs.info('Receive a regmsg:{%s}' % str(msg))
            if msg['type'] == 'register_worker_success':
                self.reg_succ_worker(msg['value'])
                self.logs.info('register successed.')
                break
        self.logs.info('Start the main process')
        global timer
        timer.cancel()
        timer = None

        tick(2.0, self.send_executor_status)

        while True:
            msg = self.listener.accept()
            self.logs.info('Receive a msg:{%s}' % str(msg))
            self.logs.info('Its value is:{%s}' % str(msg['value']))
            # print str(msg)
            # print str(msg['value'])
            self.process(msg)

app = workerBody()
app.run()

