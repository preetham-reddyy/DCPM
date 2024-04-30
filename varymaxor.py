import subprocess
import time
from datetime import datetime

# cpus_count = [16]

datasets = {
            "synthetic":{'maxor':[0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5],'minrf':0.04},
            "bmspos":{'minrf':0.04,"maxor":[0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45]},
            "mushroom":{'maxor':[0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45],'minrf':0.04}
        }

def serialize(dataset,maxor):
    return "{}_{}".format(dataset,maxor)

finished = set()

while True:
    # if len(finished) >= len(datasets)*len(cpus_count):
    #     break
    done = True
    l = open("varymaxor_new.log","a")
    for dataset in datasets:
        for maxor in datasets[dataset]['maxor']:
            cur = serialize(dataset,maxor)
            if cur in finished:
                continue
            done =  False
            cpus = 16
            minrf = datasets[dataset]['minrf']
            # maxor = datasets[dataset]['maxor']
            mempercpu = 3000
            print(dataset, cpus, mempercpu, maxor)
            proc = subprocess.Popen("/bin/sbatch --mem-per-cpu {} -c {} varymaxor_new.sh {} {} {} {}".format(mempercpu, cpus, minrf, maxor, cpus, dataset),stdout=subprocess.PIPE, shell=True)
            result,err = proc.communicate()
            exit_code = proc.wait()
            print(result,err)
            if exit_code == 0:           
                finished.add(cur)
                print("subitted succesfully")
                l.write("{}  :  dataset: {} cpus: {} mempercpu: {} maxor: {} result:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), dataset,cpus, mempercpu, maxor, result))
            else:
                print("submission failed, exit code: {}".format(exit_code))
    l.close()
    if done:
        break
    time.sleep(60)

