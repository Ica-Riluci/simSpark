import sys
import json
import threading
import time
import logging
from logging import handlers
sys.path.append('..')
from publib.SparkConn import *

class simApp:
    def __init__(self, name=None):
        self.app_name = name
        self.app_id = None
        self.status = 'WAIT'
        self.executor_list = []

class simContext:
    def __init__(self, app, port=9999):
        self.app = app
        self.driver_id = None
        self.port = port
        self.listener = SparkConn('localhost', port)
        self.rdds = []
        self.logs = logging.getLogger('simSparkLog')
        self.logs.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(
            '/tmp/simSpark_driver.log',maxBytes=10000000,backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logs.addHandler(fh)

        self.config = self.load_config()
        self.register_timeout = False

        # register app and driver
        value = {
            'host' : self.config['self_host'],
            'port' : port
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
        value = {
            'host' : self.config['self_host'],
            'port' : port,
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
                self.app.executor_list = msg['value']['executor_list']
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

    def get_rdd_by_id(self, id):
        for rdd in self.rdds:
            if rdd.rdd_id == id:
                return rdd
        return None

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
        self.rdds.append(new_rdd)
        return new_rdd        

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
            # Not implemented
    
    def __str__(self):
        return self.records.__str__()

    __repr__ = __str__

class simRDD:
    rdd_count = 0
    STORE_NONE = 0
    STORE_MEM = 1
    def __init__(self, ctx, dep=[], part=[], s_lvl=STORE_NONE):
        simRDD.rdd_count += 1
        self.context = ctx
        self.dependencies = dep
        self.rdd_id = simRDD.rdd_count
        self.partitions = part
        self.storage_lvl = s_lvl

    def iter(self, part):
        if self.storage_lvl != simRDD.STORE_NONE:
            pass
            # Not implemented
            # Assume that RDD can not be cached
        else:
            return self.compute(part)

    def _1v1dependencies(self, part):
        parent_rdd = self.context.get_rdd_by_id(self.dependencies[0])
        parent_part = parent_rdd.partitions[part.idx]
        records = parent_rdd.iter(parent_part).records
        return records

    def compute(self, part):
        return part

    def map(self, fun):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = mappedRDD(self.context, [self.rdd_id], new_parts, fun)
        return new_rdd

    def flatmap(self, fun):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = flatMappedRDD(self.context, [self.rdd_id], new_parts, fun)
        return new_rdd
    
    def filter(self, fun):
        new_parts = []
        for i in range(0, len(self.partitions)):
            new_part = simPartition(i, [], simPartition.PARENT)
            new_parts.append(new_part)
        new_rdd = filterRDD(self.context, [self.rdd_id], new_parts, fun)
        return new_rdd

    def union(self):
        pass

    def group_by_key(self):
        pass
    
    def reduce_by_key(self):
        pass
    
    def distinct(self):
        pass

    def cogroup(self):
        pass
    
    def intersection(self):
        pass

    def join(self):
        pass

    def sort_by_key(self):
        pass
    
    def cartesian(self):
        pass

    def coalesce(self):
        pass

    def repartition(self):
        pass
# transformation
    def reduce(self):
        pass
    
    def collect(self):
        pass

    def count(self):
        pass

    def foreach(self):
        pass

    def take(self):
        pass

    def first(self):
        pass

    def take_sample(self):
        pass
    
    def take_ordered(self):
        pass

    def count_by_key(self):
        pass
# action
    def __str__(self):
        out = []
        for part in self.partitions:
            out += self.compute(part).records
        return out.__str__()
    
    __repr__ = __str__

class mappedRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(mappedRDD, self).__init__(ctx, dep, part, s_lvl)
        self.func = fun

    def compute(self, part):
        recs = self._1v1dependencies(part)
        new_recs = []
        for rec in recs:
            new_recs.append(self.func(rec))
        new_part = simPartition(part.idx, new_recs)
        return new_part

class flatMappedRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(flatMappedRDD, self).__init__(ctx, dep, part, s_lvl)
        self.func = fun

    def compute(self, part):
        recs = self._1v1dependencies(part)
        new_recs = []
        for rec in recs:
            tmpresult = self.func(rec)
            if type(tmpresult) != type([]):
                self.context.logs.error('Fail to compute the partition %d in <flatMap> RDD %d.' % (recs.index(rec), self.rdd_id))
                return None
            else:
                new_recs += self.func(rec)
        new_part = simPartition(part.idx, new_recs)
        return new_part

class filterRDD(simRDD):
    def __init__(self, ctx, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(filterRDD, self).__init__(ctx, dep, part, s_lvl)
        self.func = fun
    
    def compute(self, part):
        recs = self._1v1dependencies(part)
        new_recs = []
        for rec in recs:
            if self.func(rec):
                new_recs.append(rec)
        new_part = simPartition(part.idx, new_recs)
        return new_part
