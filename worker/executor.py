import time
import threading

class executor(threading.Thread):
    def __init__(self, eid, appid, worker):
        threading.Thread.__init__(self)
        self.eid = eid
        self.appid = appid
        self.status = 'WAIT'
        self.worker = worker

    def __delete__(self, instance):
        pass

    def setId(self, rdd_id, partition_id):
        self.rdd_id = rdd_id
        self.partition_id = partition_id

    # todo   change the function into "run"
    def run(self):
        self.worker.logs.info('Executor %d start the main function' % self.eid)
        self.status = 'RUNNING'
        result = self.context.getPartition(self.rdd_id, self.partition_id)
        # store the result in rdd
        rdd = self.context.searchRdd(self.rdd_id)
        rdd.add_result(self.partition_id, result)
        # todo send the result out to the driver
        self.context.sendResult(self.rdd_id, self.partition_id)
        self.status = 'COMPLETED'

    def _buildin_map(self, x):
        if x < 4:
            return x + 1
        return x

    def _buildin_reduce(self, x, y):
        return x + y



class sparkContext(object):
    STORE_NONE = 0
    STORE_MEM = 1
    NORMAL_RDD = 0
    MAP_RDD = 1
    FLATMAP_RDD = 2
    FILTER_RDD = 3
    BUILDIN = 0
    FREESOURCE = 1

    def __init__(self, appid, worker, host, port):
        self.appid = appid
        self.driverhost = host
        self.driverport = port
        self.worker = worker
        self.RDDList = []
        # self.partitionList = []

    def getRdd(self, rddid):
        rddStatus = self.worker.fetch_info(rddid, self.driverhost, self.driverport)
        '''
        value = {
                'rdd_type' : rdd.type,
                'part_len' : len(rdd.partitions),
                'dependencies' : dep,
                'funtype' : rdd.funtype,
                'fun' : rdd.fun.__name__
        }
        '''
        type = rddStatus['rdd_type']
        if type == self.NORMAL_RDD:
            rdd = simRDD(self, rddid)
        elif type == self.MAP_RDD:
            rdd = mappedRDD(self, rddid, rddStatus['dependencies'], rddStatus['part_len'], rddStatus['fun'])
        elif type == self.FLATMAP_RDD:
            pass
        elif type == self.FILTER_RDD:
            rdd = filterRDD(self, rddid, rddStatus['dependencies'], rddStatus['part_len'], rddStatus['fun'])

        self.RDDList.append(rdd)
        return rdd

    # assume that we don't store partition data in the sparkContext
    def getPartition(self, rddid, partitionid):
        rdd = self.searchRdd(rddid)
        dependencyList = rdd.get_dependencies_list(partitionid)
        dataList = []
        if dependencyList == []:
            partition = self.worker.fetch_data(rddid, partitionid, self.driverhost, self.driverport)
        else:
            for d in dependencyList:
                dataList.append(self.getPartition(d['rdd'], d['partition']))
            partition = rdd.compute(dataList, rddid, partitionid)
        return partition

    def searchRdd(self, rddid):
        for e in self.RDDList:
            if e.rid == rddid:
                return e
        rdd = self.getRdd(rddid)
        return rdd

    # def searchPartition(self, rddid, pid):
    #     for e in self.partitionList:
    #         if e.id == rddid & e.pid == pid:
    #             return e
    #     rdd = self.getPartition(rddid, pid)
    #     return rdd

    def sendResult(self, rddid, pid):
        self.worker.send_result(rddid, pid, self.driverhost, self.driverport)

    def get_partition_data(self, rid, pid):
        rdd = self.searchRdd(rid)
        if not rdd:
            return None
        data = rdd.search_part(pid)
        return data

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

    def __init__(self, rid, ctx, dep=[], part=[], s_lvl=STORE_NONE):
        self.rid = rid
        self.context = ctx
        self.dependencies = dep
        self.partitions = part
        for p in self.partitions:
            p.set_rdd(self.rdd_id)
        self.storage_lvl = s_lvl
        self.fun = None
        self.funtype = simRDD.BUILDIN
        self.pdata = []

    @property
    def after_shuffle(self):
        return False

    @property
    def type(self):
        return simRDD.NORMAL_RDD

    def search_part(self, pid):
        for p in self.pdata:
            if p['pid'] == pid:
                return p['result']
        return None

    def add_result(self, pid, result):
        if not self.search_part(pid):
            self.pdata.append({
                'pid' : self.partition_id,
                'result': result
            })

    def _1on1_dependencies(self, part):
        return [{
            'rdd': self.dependencies[0],
            'partition': [self.dependencies[0].partitions[part.idx]]
        }]

    def get_dependencies_list(self, part):
        return []

    def compute(self, dep_list):
        return []

class mappedRDD(simRDD):
    def __init__(self, rid, ctx, dep, part, fun, ftype=simRDD.FREESOURCE, s_lvl=simRDD.STORE_NONE):
        super(mappedRDD, self).__init__(rid, ctx, dep, part, s_lvl)
        self.fun = fun
        self.funtype = ftype

    @property
    def type(self):
        return simRDD.MAP_RDD

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

    def compute(self, dep_list):
        res = []
        last_part = dep_list[0]
        for e in last_part:
            res.append(self.buildin(e))

    def buildin(self, x):
        if x < 4:
            return x + 1
        return x

class flatMappedRDD(simRDD):
    def __init__(self, rid, ctx, dep, part, fun, ftype=simRDD.FREESOURCE, s_lvl=simRDD.STORE_NONE):
        super(flatMappedRDD, self).__init__(rid, ctx, dep, part, s_lvl)
        self.fun = fun
        self.funtype = ftype

    @property
    def type(self):
        return simRDD.FLATMAP_RDD

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)

class filterRDD(simRDD):
    def __init__(self, rid, ctx, dep, part, fun, ftype=simRDD.FREESOURCE, s_lvl=simRDD.STORE_NONE):
        super(filterRDD, self).__init__(rid, ctx, dep, part, s_lvl)
        self.fun = fun
        self.funtype = ftype

    @property
    def type(self):
        return simRDD.FILTER_RDD

    def get_dependencies_list(self, part):
        return self._1on1_dependencies(part)
