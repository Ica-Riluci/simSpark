import sys
import logging
from logging import handlers
sys.path.append('..')
from publib.SparkConn import *

class simApp:
    app_count = 0
    def __init__(self, name=None):
        self.app_name = name
        self.app_id = None
        self.status = 'WAIT'
        self.executor_list = []
        simApp.app_count += 1
        if simApp.app_count > 1:
            self.valid = False
        else:
            self.valid = True

class simContext:
    def __init__(self, app, port=9999):
        self.app = app
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
            new_par = simPartition(data)
            part.append(new_par)
        new_rdd = simRDD(self, [], part)
        return new_rdd        

class simPartition:
    MEMORY = 0
    FILE = 1
    PARENT = 2
    def __init__(self, source=None, method=MEMORY):
        self.source = source
        self.method = method
    
    def get_data(self):
        if self.method == simRDD.MEMORY:
            return self.source
        else:
            pass
            # Not implemented

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
        self.context.rdds.append(self)

    def iter(self, part):
        if self.storage_lvl != simRDD.STORE_NONE:
            pass
            # Not implemented
            # Assume that RDD can not be cached
        else:
            return self.compute(part)

    def compute(self, part):
        pass

    def map(self, fun):
        new_part = []
        for i in range(0, len(self.partitions)):
            part = simPartition(None, simPartition.PARENT)
            new_part.append(part)
        new_rdd = mappedRDD(
            [self.rdd_id],
            new_part,
            fun
        )
        self.context.append(new_rdd)
        return new_rdd

    def flatmap(self, fun):
        new_part = []
        for i in range(0, len(self.partitions)):
            part = simPartition(None, simPartition.PARENT)
            new_part.append(part)
        new_rdd = flatMappedRDD(
            [self.rdd_id],
            new_part,
            fun
        )
        self.context.append(new_rdd)
        return new_rdd
    
    def filter(self, fun):
        new_part = []
        for i in range(0, len(self.partitions)):
            part = simPartition(None, simPartition.PARENT)
            new_part.append(part)
        new_rdd = filterRDD(
            [self.rdd_id],
            new_part,
            fun
        )
        self.context.append(new_rdd)
        return new_rdd

class mappedRDD(simRDD):
    def __init__(self, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(mappedRDD, self).__init__(dep, part, s_lvl)
        self.map = fun

    def compute(self, part):
        if part.method == simPartition.PARENT:
            parent_rdd = self.context.get_rdd_by_id(self.dependencies[0])
            parent_part = parent_rdd.partitions[self.partitions.index(part)]
            data = parent_rdd.iter(parent_part).get_data()
            return simPartition(self.map(data))
        else:
            return part

class flatMappedRDD(simRDD):
    def __init__(self, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(flatMappedRDD, self).__init__(dep, part, s_lvl)
        self.map = fun

    def compute(self, part):
        if part.method == simPartition.PARENT:
            parent_rdd = self.context.get_rdd_by_id(self.dependencies[0])
            parent_part = parent_rdd.partitions[self.partitions.index(part)]
            data = parent_rdd.iter(parent_part).get_data()
            return simPartition(self.map(data))
        else:
            return part

class filterRDD(simRDD):
    def __init__(self, dep, part, fun, s_lvl=simRDD.STORE_NONE):
        super(filterRDD, self).__init__(dep, part, s_lvl)
        self.map = fun
    
    def compute(self, part):
        if part.method == simPartition.PARENT:
            parent_rdd = self.context.get_rdd_by_id(self.dependencies[0])
            parent_part = parent_rdd.partitions[self.partitions.index(part)]
            data = parent_rdd.iter(parent_part).get_data()
            if self.map(data):
                return data
            else:
                return None
        else:
            return part
