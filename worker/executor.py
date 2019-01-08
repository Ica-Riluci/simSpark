import time
import threading

class executor(threading.Thread):
    def __init__(self, id, appid, worker):
        self.id = id
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
        self.status = 'RUNNING'
        result = self.context.getPartition(self.rdd_id, self.partition_id)

        # todo send the result out to the driver
        self.context.getPartition.sendResult(result)

        self.status = 'COMPLETED'

    '''
    #including the calculate and store procedure
    def calc(self, rdd_id, partition_id):
        res1 = self.worker.getPartition(partition_id, rdd_id, self.driver_host, self.driver_port)
        if (res1 != None):
            return res1
        rdd = self.worker.getRDD(rdd_id, self.driver_host, self.driver_port)
        # calculate dependency, assume that its a one to one function
        partition_data = []
        for e in rdd['dependencies']:
            tmp_res = self.worker.getPartition(partition_id, e, self.driver_host, self.driver_port)
            if (tmp_res == None):
                tmp_res = self.calc(partition_id, e)
            partition_data.append(tmp_res)
        result = self.compute(rdd['rdd_type'], rdd['part_len'], partition_data, rdd['funtype'])
        self.worker.setPartition(partition_id, rdd_id, result)
        return result

    def compute(self, type, part_len ,partition_data, func):
        result = []
        if type == 'map':
            for e in partition_data[0]:
                result.append(self._buildin_map(e))
        elif type == 'reduce':
            length = len(result)
            for i in range(0, length):
                result.append(self._buildin_reduce(partition_data[0][i], partition_data[1][i]))
        return result
    '''

    def _buildin_map(self, x):
        if x < 4:
            return x + 1
        return x

    def _buildin_reduce(self, x, y):
        return x + y



class sparkContext(object):
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
        rdd =
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
                dataList.append(self.getPartition(d.rddid, d.partitionid))
            partition = rdd.compute(dataList, rddid, partitionid)
        return partition

    def searchRdd(self, rddid):
        for e in self.RDDList:
            if e.id == rddid:
                return e
        rdd = self.getRdd(rddid)
        return rdd

    def searchPartition(self, rddid, pid):
        for e in self.partitionList:
            if e.id == rddid & e.pid == pid:
                return e
        rdd = self.getPartition(rddid, pid)
        return rdd

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
            'rdd': self.dependencies[0],
            'partition': [self.dependencies[0].partitions[part.idx]]
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