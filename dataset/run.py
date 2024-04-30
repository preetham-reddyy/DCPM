import sys
f = open(sys.argv[1],'r')
fout = open(sys.argv[2],'w') 
line = f.readline()

while line!='':
	line = line.strip()
	line = line.split(" ")
	line = ",".join(line)
	fout.write(str(line)+"\n")
	line = f.readline()
fout.close()
f.close() 
