import os

# bmspos
data = "bmspos"
print data
for i in range(10,100,10):
   maxOR = str(i/100.0)
   os.system("python cmine_mapreduce.py 0.075 0.4 "+maxOR+" 20 ../dataset/"+data+".txt "+data)

for i in range(10,100,10):
   minCS = str(i/100.0)
   os.system("python cmine_mapreduce.py 0.075 "+minCS+" 0.7 20 ../dataset/"+data+".txt "+data)

# mushroom
# data = "mushroom"
# print data
# for i in range(10,100,10):
#     maxOR = str(i/100.0)
#     os.system("python cmine_mapreduce.py 0.3 0.3 "+maxOR+" 10 ../dataset/"+data+".txt "+data)

# for i in range(10,100,10):
#     minCS = str(i/100.0)
#     os.system("python cmine_mapreduce.py 0.3 "+minCS+" 0.5 10 ../dataset/"+data+".txt "+data)

# synthetic
data = "synthetic"
print data
for i in range(45,51,5):
    maxOR = str(i/100.0)
    os.system("python cmine_mapreduce.py 0.045 0.3 "+maxOR+" 20 ../dataset/"+data+".txt "+data)

for i in range(10,100,10):
    minCS = str(i/100.0)
    os.system("python cmine_mapreduce.py 0.045 "+minCS+" 0.25 20 ../dataset/"+data+".txt "+data)

#kosarak
# data = "kosarak"
# print data
# for i in range(10,100,10):
#     maxOR = str(i/100.0)
#     os.system("python cmine_mapreduce.py 0.025 0.5 "+maxOR+" 10 ../dataset/"+data+".txt "+data)

# for i in range(10,100,10):
#     minCS = str(i/100.0)
#     os.system("python cmine_mapreduce.py 0.025 "+minCS+" 0.5 10 ../dataset/"+data+".txt "+data)

