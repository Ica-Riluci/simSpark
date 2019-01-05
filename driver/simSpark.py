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
            new_par = simPartition(i, data)
            part.append(new_par)
        new_rdd = simRDD(self, [], part)
        new_rdd._register()
        return new_rdd

    def search_stage_by_rdd(self, rid):
        for s in self.undone:
            if s.rdd.rdd_id == rid:
                return s
        return None
    
    def search_rdd_by_id(self, rid):
        for r in self.rdds:
            if r.rdd_id == rid:
                return r
        return None


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
    def __init__(self, idx, source=None, method=MEMORY):
        self.source = source
        self.method = method
        self.idx = idx
    
    @property
    def records(self):
        if self.method == simPartition.MEMORY:
            return self.source
        else:
            pass
    
    def __str__(self):
        return self.records.__str__()

    __repr__ = __str__


class simRDD:
    rdd_count = 0
    
    STORE_NONE = 0
    STORE_MEM = 1

    def __init__(self, ctx, dep=[], part=[], s_lvl=STORE_NONE):
        self.context = ctx
        self.dependencies = dep
        self.partitions = part
        self.storage_lvl = s_lvl

    @property
    def after_shuffle(self):
        return False 

    def _register(self):
        simRDD.rdd_count += 1
        self.rdd_id = simRDD.rdd_count
        self.context.rdds.append(self)

    def _map(self, fun):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = mappedRDD(self.context, [self.rdd_id], new_parts, fun)
        return new_rdd

    def map(self, fun):
        ret = self._map(fun)
        ret._register()
        return ret

    def _flatmap(self, fun):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = flatMappedRDD(self.context, [self.rdd_id], new_parts, fun)
        return new_rdd
    
    def flatmap(self, fun):
        ret = self._flatmap(fun)
        ret._register()
        return ret

    def _filter(self, fun):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = filterRDD(self.context, [self.rdd_id], new_parts, fun)
        return new_rdd

    def filter(self, fun):
        ret = self._filter(fun)
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

class mappedRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(mappedRDD, self).__init__(ctx, dep, part, s_lvl)
        self.func = fun

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

class flatMappedRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(flatMappedRDD, self).__init__(ctx, dep, part, s_lvl)
        self.func = fun

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

class filterRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(filterRDD, self).__init__(ctx, dep, part, s_lvl)
        self.func = fun

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

class simStage:
    stage_count = 0
    def __init__(self, ctx, finalrdd):
        simStage.stage_count += 1
        self.stage_id = simStage.stage_count
        self.context = ctx
        self.rdd = finalrdd
        self.done = False
        self.submitted = False
        self.parent_stage = []
    
    def schedule(self):
        ancestors = []
        for part in self.rdd.partitions:
            ancestors += self.rdd.ancestor(part)
        ancestors = list(set(ancestors))
        for rid in ancestors:
            if not self.context.search_stage_by_rdd(rid):
                new_stage = simStage(self.context, self.context.search_rdd_by_id(rid))
                self.parent_stage.append(new_stage)