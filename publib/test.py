from SparkConn import *
import threading
import time
import json

listener = SparkConn('127.0.0.1', 11111)
sender = SparkConn('127.0.0.1', 11112)

global timer
timer = None

def ptime():
  sender.sendMessage(wrap_msg('172.21.0.12', 11111, 'test', time.ctime()))
  tick()

def tick():
  global timer
  if timer:
    timer.finished.wait(1)
    timer.function()
  else:
    timer = threading.Timer(1, ptime)
    timer.start()

def wrap_msg(address, port, type, value):
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

tick()
while True:
  msg = listener.accept()
  print(msg['value'])
