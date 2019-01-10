import socket
import json

class SparkConn:
    __host__ = socket.gethostbyname(socket.gethostname())
    __port__ = 12345
    __s__ = None
    __value__ = None

    def __init__(self, hostname, portname):
        self.host = hostname
        self.port = portname
        # error checking
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.host, self.port))
        self.s.listen(5)

    def __del__(self):
        self.close()

    # return a dict with type and value 2 attributes
    def accept(self):
        c, addr = self.s.accept()
        resp = str(c.recv(4096))
        c.close()
        jresp = json.loads(resp)
        pyDict = json.loads(jresp['value'])
        return pyDict

    def getHost(self):
        return self.host

    def getPort(self):
        return self.port

    def close(self):
        self.s.close()

    # send a message without a file, if succeed return True, else return False
    def sendMessage(self, pyDict):
        aimHost = pyDict['host']
        aimPort = pyDict['port']
        self.value = json.dumps(pyDict)
        if len(self.value) > 4096:
            return False
        else:
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((aimHost, aimPort))
            flag = c.sendall(self.value)
            if flag == None:
                return True
            else:
                return False




