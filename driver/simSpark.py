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
        # print('A new app <%s>' % name)
        self.app_name = name
        self.app_id = None
        self.status = 'WAIT'
        self.idle_executors = []
        # self.busy_executors = []
    
    @property
    def busy(self):
        return not len(self.idle_executors) > 0

class backendComm(threading.Thread):
    def __init__(self, ctx):
        threading.Thread.__init__(self)
        self.context = ctx
        self.running = threading.Event()
        self.running.set()

    def query_rdd(self, q):
        rdd = self.context.search_rdd_by_id(q['rid'])
        if not rdd:
            self.lis.sendMessage(self.context.wrap_msg(
                q['host'],
                q['port'],
                'Non-exist_rdd',
                q['rid']
            ))
        else:
            dep = []
            for d in rdd.dependencies:
                dep.append(d.rdd_id)
            value = {
                'rdd_type' : rdd.type,
                'part_len' : len(rdd.partitions),
                'dependencies' : dep,
                'funtype' : rdd.funtype,
                'fun' : rdd.fun.__name__
            }
            self.lis.sendMessage(self.context.wrap_msg(
                q['host'],
                q['port'],
                'fetch_info_ack',
                value
            ))

    def query_partition(self, q):
        rdd = self.context.search_rdd_by_id(q['rid'])
        if not rdd:
            self.lis.sendMessage(self.context.wrap_msg(
                q['host'],
                q['port'],
                'Non-exist_rdd',
                q['rid']
            ))
        else:
            if q['pidx'] >= len(rdd.partitions):
                self.lis.sendMessage(self.context.wrap_msg(
                    q['host'],
                    q['port'],
                    'Non-exist_partition',
                    {
                        'rid' : q['rid'],
                        'pidx' : q['pidx']
                    }
                ))
            else:
                if not rdd.parititions[q['pidx']].fetchable:
                    self.lis.sendMessage(self.context.wrap_msg(
                        q['host'],
                        q['port'],
                        'not_stored_partition',
                        {
                            'rid' : q['rid'],
                            'pidx' : q['pidx']
                        }
                    ))
                else:
                    self.lis.sendMessage(self.context.wrap_msg(
                        q['host'],
                        q['port'],
                        'fetch_data_ack',
                        rdd.parititions[q['pidx']].records
                    ))

    def update_task(self, u):
        rdd = self.context.search_rdd_by_id(u['rid'])
        if not rdd:
            self.lis.sendMessage(self.context.wrap_msg(
                u['host'],
                u['port'],
                'Non-exist_rdd',
                u['rid']
            ))
        else:
            if u['pidx'] >= len(rdd.partitions):
                self.lis.sendMessage(self.context.wrap_msg(
                    u['host'],
                    u['port'],
                    'Non-exist_partition',
                    {
                        'rid' : u['rid'],
                        'pidx' : u['pidx']
                    }
                ))
            else:
                rdd.partitions[u['pidx']].update_source((u['host'], u['port']))
                stage = self.context.search_stage_by_rdd(rdd.rdd_id)
                if not stage:
                    self.context.logs.critical('Missing stage.')
                    return
                stage.task_done[u['pidx']] = True
                if stage.done:
                    stage.finish()
                self.lis.sendMessage(self.context.wrap_msp(
                    u['host'],
                    u['port'],
                    'task_finished_ack',
                    {
                        'rid' : u['rid'],
                        'pidx' : u['pidx']
                    }
                ))

    def dispense(self, msg):
        if msg['type'] == 'fetch_info':
            self.query_rdd(msg['value'])
        elif msg['type'] == 'fetch_data':
            self.query_partition(msg['value'])
        elif msg['type'] == 'task_finished':
            self.update_task(msg['value'])

    def run(self):
        self.lis = SparkConn(self.context.config['driver_host'], self.context.bport)
        while self.running.is_set():
            msg = self.lis.accept()
            self.dispense(msg)

    def collapse(self):
        self.running.clear()
                

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
        self.bport = self.config['backend_port']
        self.parallel_stage = self.config['parallel_stage']
        self.rdds = []
        self.undone = []
        self.stages = []
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
        self.logs.info('Waiting for registeration feedback')
        while True:
            msg = self.listener.accept()
            if msg['type'] == 'register_driver_success':
                self.driver_id = msg['value']['id']
                break
        # register app
        value = {
            'host' : self.config['driver_host'],
            'port' : self.port,
            'did' : self.driver_id,
            'name' : self.app.app_name
        }
        self.listener.sendMessage(self.wrap_msg(
            self.config['master_host'],
            self.config['master_port'],
            'register_app',
            value
        ))
        self.logs.info('Wait for registeration feedback')
        while True:
            msg = self.listener.accept()
            if msg['type'] == 'resource_update':
                self.app.app_id = msg['value']['id']
                self.app.idle_executor = msg['value']['idle_executor']
                self.app.busy_executor = msg['value']['busy_executor']
                break
        self.comm = backendComm(self)
        self.comm.start()

    def __del__(self):
        value = {
            'id' : self.app.app_id,
            'driver_id' : self.driver_id    
        }
        self.listener.sendMessage(self.wrap_msg(
            self.config['master_host'],
            self.config['master_port'],
            'kill_app',
            value
        ))
        while True:
            msg = self.listener.accept()
            if msg['type'] == 'app_killed':
                break
        value = self.driver_id
        self.listener.sendMessage(self.wrap_msg(
            self.config['master_host'],
            self.config['master_port'],
            'kill_driver',
            value
        ))
        self.comm.collapse()

    def wrap_msg(self, address, port, type, value):
        raw = {
            'type' : type,
            'value' : value
        }
        wrapped = {
            'host' : address,
            'port' : port,
            'value' : json.dumps(raw)
        }
        return wrapped

    def load_config(self):
        self.logs.info('<driver_config.json> is about to be loaded.')
        config = {
            'master_host' : '172.21.0.12',
            'master_port' : 11111,
            'driver_host' : '172.21.0.3',
            'driver_port' : 10001,
            'backend_port' : 10002,
            'parallel_stage' : 1,
            'timeout' : 60
        }
        try:
            with open('driver_config.json', 'r') as jsoninput:
                inp = json.load(jsoninput)
            for k in config.keys():
                if k in inp.keys():
                    config[k] = inp[k]
        except IOError:
            self.logs.warning('Failed to read configuration. Use default instead.')
        return config

    def parallelize(self, arr, fineness=-1):
        part = []
        if fineness == 0 or fineness < -1:
            self.logs.error('Invalid arguments of parallelism. Failed to create RDD')
            return None
        if fineness == -1:
            fineness = len(arr)
        for i in range(0, fineness):
            l = len(arr) // fineness * i
            r = len(arr) // fineness * (i + 1)
            data = arr[l:r]
            new_par = simPartition(self, i, data)
            part.append(new_par)
        new_rdd = simRDD(self, [], part)
        new_rdd._register()
        return new_rdd

    def search_stage_by_rdd(self, rid):
        for s in self.stages:
            if s.rdd.rdd_id == rid:
                return s
        return None
    
    def search_rdd_by_id(self, rid):
        for r in self.rdds:
            if r.rdd_id == rid:
                return r
        return None

    def resource_request(self, n=1):
        value = {
            'driver_id' : self.driver_id,
            'number' : n
        }
        self.listener.sendMessage(self.wrap_msg(
            self.config['master_host'],
            self.config['master_port'],
            'request_resource',
            value
        ))

    @property
    def ready_stages(self):
        ret = []
        for stage in self.undone:
            ready = True
            for pstage in stage.parent_stage:
                if pstage in self.undone:
                    ready = False
                    break
            if ready:
                ret.append(stage)
                if self.parallel_stage > 0  and len(ret) == self.parallel_stage:
                    return ret
        return ret

    def fetch_partition(self, source, rid, pidx, frommem):
        if not rid:
            return None
        if frommem:
            value = {
                'appid' : self.app.app_id,
                'host' : self.config['driver_host'],
                'port' : self.config['driver_port'],
                'rid' : rid,
                'pidx' : pidx
            }
            self.listener.sendMessage(self.wrap_msg(
                source[0],
                source[1],
                'fetch_data',
                value
            ))
            while True:
                msg = self.listener.accept()
                if msg['type'] == 'fetch_data_ack':
                    return msg['value']
        return None

    def list_clear(self, stages):
        for stage in stages:
            if not stage.done:
                return False
        return True

    def pend_task(self, executor, rid, pidx):
        value = {
            'eid' : executor['executor_id'],
            'rid' : rid,
            'pidx' : pidx,
            'host' : self.config['driver_host'],
            'port' : self.config['backend_port']
        }
        self.listener.sendMessage(self.wrap_msg(
            executor['host'],
            executor['port'],
            'pending_task',
            value
        ))


# for test
def _buildin_map(self, x):
    if x < 4:
        return x + 1
    return x
    
def _buildin_reduce(self, x, y):
    return x + y

class simPartition:
    MEMORY = 0
    FILE = 1
    PARENT = 2
    def __init__(self, ctx, idx, source=None, method=MEMORY, local=True):
        self.source = source
        self.method = method
        self.local = local
        self.idx = idx
        self.context = ctx
        self.rdd_id = None
    
    @property
    def records(self):
        if self.method == simPartition.MEMORY:
            if self.local:
                return self.source
            else:
                self.local = True
                data = self.context.fetch_partition(self.source, self.rdd_id, self.idx, frommem=True)
                self.source = data
                return data
        else:
            pass

    @property
    def fetchable(self):
        return self.local

    def update_source(self, s):
        if self.local:
            return
        self.source = s

    def set_rdd(self, rid):
        self.rdd_id = rid
    
    def __str__(self):
        return self.records.__str__()

    __repr__ = __str__


class simRDD:
    rdd_count = 0
    
    STORE_NONE = 0
    STORE_MEM = 1
    NORMAL_RDD = 0
    MAP_RDD = 1
    FLATMAP_RDD = 2
    FILTER_RDD = 3
    BUILDIN = 0
    FREESOURCE = 1

    def __init__(self, ctx, dep=[], part=[], s_lvl=STORE_NONE):
        self.context = ctx
        self.dependencies = dep
        self.partitions = part
        for p in self.partitions:
            p.set_rdd(self.rdd_id)
        self.storage_lvl = s_lvl
        self.fun = None
        self.funtype = simRDD.BUILDIN

    @property
    def after_shuffle(self):
        return False

    @property
    def type(self):
        return simRDD.NORMAL_RDD

    def _register(self):
        simRDD.rdd_count += 1
        self.rdd_id = simRDD.rdd_count
        self.context.rdds.append(self)

    def _map(self, fun, ftype=FREESOURCE):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(self.context, i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = mappedRDD(self.context, [self.rdd_id], new_parts, fun, ftype)
        return new_rdd

    def map(self, fun, ftype=FREESOURCE):
        ret = self._map(fun, ftype)
        ret._register()
        return ret

    def _flatmap(self, fun, ftype=FREESOURCE):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(self.context, i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = flatMappedRDD(self.context, [self.rdd_id], new_parts, fun, ftype)
        return new_rdd
    
    def flatmap(self, fun, ftype=FREESOURCE):
        ret = self._flatmap(fun, ftype)
        ret._register()
        return ret

    def _filter(self, fun, ftype=FREESOURCE):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(self.context, i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = filterRDD(self.context, [self.rdd_id], new_parts, fun, ftype)
        return new_rdd

    def filter(self, fun, ftype=FREESOURCE):
        ret = self._filter(fun, ftype)
        ret._register()
        return ret

    def _1on1_dependencies(self, part):
        return [{
            'rdd' : self.dependencies[0],
            'partition' : [self.dependencies[0].partitions[part.idx]]
        }]

    def get_dependencies_list(self, part):
        return []

    def ancestor(self, part):
        dep = self.get_dependencies_list(part)
        ret = []
        if self.after_shuffle:
            for dependency in dep:
                ret.append(dependency['rdd'].rdd_id)
            return ret
        else:
            for dependency in dep:
                for part in dependency['partition']:
                    ret += dependency['rdd'].ancestor(part)
            return list(set(ret))

    def calc(self):
        final_stage = simStage(self.context, self)
        final_stage.schedule()
        final_stage.register()
        while len(self.context.ready_stages) > 0:
            stages = self.context.ready_stages
            for stage in stages:
                stage.boot()
            while not self.context.list_clear(stages):
                continue
    
    # actions
    def reduce(self, fun):
        self.calc()
        while not self.context.search_stage_by_rdd(self).done:
            continue
        col = []
        for part in self.partitions:
            ret = part.records[0]
            restrec = part.records[1:]
            for rec in restrec:
                ret = fun(ret, rec)
            col.append(ret)
        if len(col) <= 0:
            return None
        ret = col[0]
        for rec in col[1:]:
            ret = fun(ret, rec)
        return ret
            

class mappedRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, ftype=simRDD.FREESOURCE, s_lvl=simRDD.STORE_NONE):
        super(mappedRDD, self).__init__(ctx, dep, part, s_lvl)
        self.fun = fun
        self.funtype = ftype

    @property
    def type(self):
        return simRDD.MAP_RDD

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

class flatMappedRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, ftype=simRDD.FREESOURCE, s_lvl=simRDD.STORE_NONE):
        super(flatMappedRDD, self).__init__(ctx, dep, part, s_lvl)
        self.fun = fun
        self.funtype = ftype

    @property
    def type(self):
        return simRDD.FLATMAP_RDD

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

class filterRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, ftype=simRDD.FREESOURCE, s_lvl=simRDD.STORE_NONE):
        super(filterRDD, self).__init__(ctx, dep, part, s_lvl)
        self.fun = fun
        self.funtype = ftype

    @property
    def type(self):
        return simRDD.FILTER_RDD

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

class simStage:
    stage_count = 0
    def __init__(self, ctx, finalrdd):
        simStage.stage_count += 1
        self.stage_id = simStage.stage_count
        self.context = ctx
        self.rdd = finalrdd
        self.parent_stage = []
        self.task_done = [False] * len(self.rdd.partitions)
    
    def schedule(self):
        ancestors = []
        for part in self.rdd.partitions:
            ancestors += self.rdd.ancestor(part)
        ancestors = list(set(ancestors))
        for rid in ancestors:
            if not self.context.search_stage_by_rdd(rid):
                new_stage = simStage(self.context, self.context.search_rdd_by_id(rid))
                self.parent_stage.append(new_stage)

    def register(self):
        for pstage in self.parent_stage:
            if not self.context.search_stage_by_rdd(pstage.rdd.rdd_id):
                pstage.register()
        self.context.undone.append(self)
        self.context.stages.append(self)
        
    def boot(self):
        self.context.resource_request(len(self.rdd.partitions))
        while True:
            msg = self.context.listener.accept()
            if msg['type'] == 'resource_ready':
                self.context.idle_executors += msg['value']
                break
        for part in self.rdd.partitions:
            while True:
                if self.context.app.busy:
                    self.context.resource_request()
                    while True:
                        msg = self.context.listener.accept()
                        if msg['type'] == 'resource_ready':
                            self.context.idle_executors += msg['value']
                        break
                    continue
                executor = self.context.app.idle_executors.pop()
                self.context.pend_task(executor, self.rdd.rdd_id, part.idx)
                # self.context.app.busy_executors.append(executor)
                break

    @property
    def done(self):
        return not False in self.task_done

    def finish(self):
        self.context.undone.remove(self)