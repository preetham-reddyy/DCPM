import subprocess
import time
from datetime import datetime

cpus_count = [4,8,12,20,24]

datasets = {"synthetic":{'maxor':0.25,'minrf':0.04},"bmspos":{'minrf':0.04,"maxor":0.4},"mushroom":{'maxor':0.35,'minrf':0.04}}

def serialize(dataset,cpus):
    return "{}_{}".format(dataset,cpus)


finished = set()

while True:
    if len(finished) >= len(datasets)*len(cpus_count):
        break
    l = open("varycpus.log","a")
    for dataset in datasets:
        for cpus in cpus_count:
            cur = serialize(dataset,cpus)
            if cur in finished:
                continue
            minrf = datasets[dataset]['minrf']
            maxor = datasets[dataset]['maxor']
            mempercpu = 8192 if cpus < 16 else 3000
            print(dataset, cpus, mempercpu)
            proc = subprocess.Popen("/bin/sbatch --mem-per-cpu {} -c {} varycpus_old.sh {} {} {} {}".format(mempercpu, cpus, minrf, maxor, cpus, dataset),stdout=subprocess.PIPE, shell=True)
            result,err = proc.communicate()
            exit_code = proc.wait()
            print(result,err)
            if exit_code == 0:           
                finished.add(cur)
                print("subitted succesfully")
                l.write("{}  :  dataset: {} cpus: {} mempercpu: {} result:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), dataset,cpus, mempercpu, result))
            else:
                print("submission failed, exit code: {}".format(exit_code))
    l.close()
    time.sleep(60)

