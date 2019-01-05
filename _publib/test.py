from simSpark import *

def extendto3(i):
   res = [i]
   while i < 3:
      i += 1
      res.append(i)
   return res

app = simApp('test')
sc = simContext(app)
rdd1 = sc.parallelize([0, 1, 2, 3, 4, 5])
rdd2 = rdd1.map(lambda x: x + 1)
rdd3 = rdd1.flatmap(extendto3)
rdd4 = rdd1.filter(lambda x: x >= 3)
print(rdd1)
print(rdd2)
print(rdd3)
print(rdd4)
