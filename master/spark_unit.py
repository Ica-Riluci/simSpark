from datetime import datetime, timedelta

class SparkUnit:
    def __init__(self, address, port):
        self.host = address
        self.port = port

class ApplicationUnit(SparkUnit):
    app_count = 0
    def __init__(self, address, port, name, did):
        ApplicationUnit.app_count += 1
        super(ApplicationUnit, self).__init__(address, port)
        self.app_name = name
        self.driver_id = did
        self.executors_req = -1
        self.executor_list = []
        self.state = 'WAIT'
        self.app_id = ApplicationUnit.app_count

class DriverUnit(SparkUnit):
    driver_count = 0
    def __init__(self, address, port):
        DriverUnit.driver_count += 1
        super(DriverUnit, self).__init__(address, port)
        self.driver_id = DriverUnit.driver_count
        self.app_id = None
    
    def set_app_id(self, aid=None):
        self.app_id = aid
        
class ExecutorUnit(SparkUnit):
    executor_count = 0
    def __init__(self, address, port, wid, aid):
        ExecutorUnit.executor_count += 1
        super(ExecutorUnit, self).__init__(address, port)
        self.executor_id = ExecutorUnit.executor_count
        self.worker_id = wid
        self.app_id = aid
        self.state = 'WAIT'

class WorkerUnit(SparkUnit):
    worker_count = 0
    def __init__(self, address, port):
        WorkerUnit.worker_count += 1
        super(WorkerUnit, self).__init__(address, port)
        self.worker_id = WorkerUnit.worker_count
        self.alive = True
        self.last_heartbeat = datetime.now()
        self.executor_list = []

    def heartbeat_expired(self, lim):
        return self.last_heartbeat + timedelta(seconds=lim) < datetime.now()

    def dead(self, lim, it):
        return self.last_heartbeat + timedelta(seconds=(lim * it)) < datetime.now()

    def awake(self):
        self.alive = True

    def update_heartbeat(self, hb):
        self.last_heartbeat = hb