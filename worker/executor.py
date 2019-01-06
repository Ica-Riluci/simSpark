import time
import worker

class executor:
    def __init__(self, id, appid, worker, dhost, dport):
        self.id = id
        self.appid = appid
        self.status = 'WAIT'
        self.worker = worker
        self.driver_host = dhost
        self.driver_port = dport

    def __delete__(self, instance):
        pass

    def runExecutor(self, rdd_id, partition_id):
        self.status = 'RUNNING'
        result = self.calc(rdd_id, partition_id)
        self.worker.sendback(rdd_id, partition_id, result)
        self.status = 'COMPLETED'

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


    def _buildin_map(self, x):
        if x < 4:
            return x + 1
        return x

    def _buildin_reduce(self, x, y):
        return x + y