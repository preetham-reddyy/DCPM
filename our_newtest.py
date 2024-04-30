# from multiprocessing import Value, Pool, Lock, Manager, Process
from collections import Counter, defaultdict
from itertools import combinations
import time, sys
import logging
import os
from pathlib import Path
from pyspark import SparkConf, SparkContext
# import cPickle as pickle
import pickle
# Path(sys.argv[6]).mkdir(parents=True, exist_ok=True)
import os
try: 
    os.makedirs(sys.argv[6])
except OSError:
    if not os.path.isdir(sys.argv[6]):
        raise

# transactions = None
# numOfProcesses = None
# candidate_set = None
# page_to_transactions = None
# max_or = None
# max_cs = None
global page_to_transactions
page_to_transactions = None


folderPath = sys.argv[6]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(folderPath,"our_mine_minrf_{}.log".format(sys.argv[2]))),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_transactions_data(file_path):
    lines = open(file_path,'r').read().splitlines()
    return [list(filter(len,line.strip().split(','))) for line in lines if len(line.strip()) > 0]

def initial_map(start_pos, transactions, numOfProcesses):
    # global numOfProcesses
    # print(transactions)
    # print(transactions.value)
    # t = [p[0].split() for p in transactions.value]
    end_pos = start_pos + min(int(len(transactions.value)/numOfProcesses.value),len(transactions.value)-start_pos)
    counter = Counter()
    cur_page_to_transactions = defaultdict(set)
    for i in range(start_pos, end_pos):
        counter.update(transactions.value[i])
        for p in transactions.value[i]:
            cur_page_to_transactions[p].add(i)
    return counter, cur_page_to_transactions

def get_intersection_len(set1, set2):
    return sum(1 for i in set2 if i in set1)

def check_conditions(set1, set2):
    # global page_to_transactions
    # global max_or
    # global max_cs
    # global transactions
    intersection_len = get_intersection_len(set1, set2)
    union_len = len(set1) + len(set2) - intersection_len
    return union_len/len(transactions), intersection_len/len(set2) 

def getSecondLevelCandidateSet(max_or, max_cs, transactions_len, first_level_patterns):
    global page_to_transactions
    # global max_or
    # global max_cs
    # global transactions
    pages = first_level_patterns.most_common()
    second_level_patterns = defaultdict(list)
    # import pdb; pdb.set_trace()
    for i in range(0, len(pages)):
        # second_level_patterns[tuple([pages[i][0]])] = []#pages[j][0] for j in range(i+1, len(pages)) if check_conditions(page_to_transactions[pages[i][0]], page_to_transactions[pages[j][0]]) ]
        new_index = tuple([pages[i][0]])
        new_list = []
        for j in range(i+1,len(pages)):
            intersection_len = get_intersection_len(page_to_transactions.value[pages[i][0]],page_to_transactions.value[pages[j][0]])
            union_len = len(page_to_transactions.value[pages[i][0]]) + len(page_to_transactions.value[pages[j][0]]) - intersection_len
            cur_or = intersection_len/len(page_to_transactions.value[pages[j][0]])
            cur_cs = union_len
            # print("check2", cur_or, cur_cs, max_or, new_index, pages[j][0])
            # if pages[j][0] == '684' or pages[j][0] == '766':
                # print("check", cur_or, cur_cs, max_or, new_index, pages[j][0])
            if cur_or <= max_or.value:
                new_list.append({
                    'page':pages[j][0],
                    'overlap':cur_or,
                    'coverage':cur_cs
                })
        second_level_patterns[new_index] = new_list
    return [second_level_patterns]

# (a,b,c), (a,b,d), (b,c,d)

# {('a','b'):[{'page':'c','overlap':x,'coverage':y}, {'page':'d','overlap':x,'coverage':y}, ('b','c'): [{'page':'d','overlap':x,'coverage':y}]]}

def getNextLevelCandidateSet(same_part_key, transactions_len, same_part_key_value):
    global page_to_transactions
    # global transactions
    # global max_or
    # global max_cs
    # global cur_level_patterns
    
    # for same_part_key in prev_level_patterns:
    # print(same_part_key,"started")
    # print(type(same_part_key))
    # print(type(same_part_key_value))
    new_patterns = dict()
    candidateset = []
    coverage_patterns = []
    if True:
        cur_set = set().union(*[page_to_transactions.value[same_part_key[i]] for i in range(0,len(same_part_key))])
        for i in range(len(same_part_key_value)):
            ith_page = same_part_key_value[i]['page']
            set_with_i = cur_set.union(page_to_transactions.value[ith_page])
            new_same_part_key = same_part_key + tuple([ith_page])
            cur_new_patterns = []
            for j in range(i+1,len(same_part_key_value)):
                new_page = same_part_key_value[j]['page']
                intersection_len = get_intersection_len(set_with_i, page_to_transactions.value[new_page])
                union_len = len(set_with_i) + len(page_to_transactions.value[new_page]) - intersection_len
                cur_or = float(intersection_len)/len(page_to_transactions.value[new_page])
                cur_cs = float(union_len)
                # if level == 4:
                    # print(new_same_part_key, new_page, cur_cs, cur_or, max_or.value)
                if cur_or <= max_or.value:
                    # print("inserting")
                    cur_new_patterns.append({
                        'page':new_page,
                        'overlap':cur_or,
                        'coverage':cur_cs,
                        'max_or':max_or.value
                    })
                    # coverage_patterns.append(coverage_patterns)
                # candidateset.append([same_part_key,ith_page, new_page])
            if len(cur_new_patterns) != 0:
                new_patterns[new_same_part_key] = cur_new_patterns
    # candidateset_size = len(pickle.dumps(candidateset))
    # coverage_patterns_size = len(pickle.dumps(coverage_patterns))
    # print("candidate set size: {}".format(candidateset_size))
    # print("coverage patterns size: {}".format(coverage_patterns_size))
    return new_patterns #, candidateset_size, coverage_patterns_size
    # print(same_part_key,"done")
    # return new_patterns
    # same_part_map = defaultdict(list) # {'a':['b','c','d']}
    # for pattern in prev_level_patterns:
    #     same_part_map[pattern[:-1]].append(pattern[-1])
    # for same_part_key in same_part_map:
    #     new_patterns += [ same_part_key + new_part for new_part in combinations(same_part_map[same_part_key],2) ]
    # return new_patterns

def getNextLevelCandidateSetMapper(args):
    return getNextLevelCandidateSet(*args)

# def get_no_patterns(start_pos):
#     global numOfProcesses
#     global page_to_transactions
#     global max_or
#     global max_cs
#     global transactions
#     end_pos = start_pos + min(int(len(candidate_set)/numOfProcesses),len(candidate_set)-start_pos)
#     no_patterns = {}
#     for i in range(start_pos, end_pos):
#         cur_set = set().union(*[page_to_transactions[candidate_set[i][j]] for j in range(0,len(candidate_set[i]) -1)])
#         intersection_len = get_intersection_len(cur_set, page_to_transactions[candidate_set[i][-1]])
#         logger.debug("intersection length of {} and {}  = {}".format([candidate_set[i][j] for j in range(0,len(candidate_set[i]) -1)],  candidate_set[i][-1],intersection_len))
#         logger.debug("{} {}".format(cur_set, page_to_transactions[candidate_set[i][-1]] ))
#         union_len = len(cur_set) + len(page_to_transactions[candidate_set[i][-1]]) - intersection_len
#         if intersection_len/len(page_to_transactions[candidate_set[i][-1]]) <= max_or:
#         	no_patterns[candidate_set[i]] = [union_len, intersection_len]
#     return no_patterns
        
def get_start_pos(count, step):
    return list(range(0, count, step))

def save_first_level_patterns(file_path, first_level_patterns):
    # return
    with open(file_path,'w') as f:
        for p in first_level_patterns:
            # logger.info(p)
            f.write("{} | {} | {}\n".format(p,first_level_patterns[p],0))
            
def save_nth_level_patterns(file_path, patterns):
    # return
    count = sum([len(group[same_key]) for group in patterns for same_key in group])
    logger.info("found {} patterns".format(count))
    # return
    # print(patterns)
    with open(file_path,'w') as f:
        for group in patterns:
            for same_key in group:
    #         # print(same_key, patterns[same_key])
            # logger.info(patterns[same_key])
                for pattern in group[same_key]:
                    # logger.info("{} | {}".format(same_key, pattern))
                    f.write("{}, {} | {} | {}\n".format(', '.join(map(str,same_key)), pattern['page'], pattern['coverage'], pattern['overlap']))


def args_yielder(cur_level_patterns, transactions_len):
    for group in cur_level_patterns:
        for same_part_key in group:
            yield same_part_key, transactions_len, group[same_part_key]

def ParallelCmine(sc, inputFile, min_rf, folderPath, numOfProcesses):
    # global transactions
    # global numOfProcesses
    # global candidate_set
    global page_to_transactions
    
    logger.info("started mining")
    
    logger.info("reading transactional data")
    transactions = get_transactions_data(inputFile)
    #logger.info("transactions size: {}".format(len(pickle.dumps(transactions))))
    
    # transactions = sc.textFile(inputFile)
    # import pdb; pdb.set_trace()
    # transactions.map(lambda x:x.strip().encode("ascii", "ignore").split(','))

# transactions = None
# numOfProcesses = None
# # candidate_set = None
# page_to_transactions = None
# max_or = None
# max_cs = None
    logger.info("First level mining")
    total_broadcast_size = 0
    # total_broadcast_size += len(pickle.dumps(transactions))
    transactions = sc.broadcast(transactions)


    transactions_len = sc.broadcast(len(transactions.value))
    numOfProcesses = sc.broadcast(numOfProcesses)
    # max_or = sc.broadcast(max_or)
    # max_cs = sc.broadcast(max_cs)
    
    web_page_counters = Counter()
    page_to_transactions = defaultdict(set)

    indices = sc.parallelize(get_start_pos(transactions_len.value,int(transactions_len.value/numOfProcesses.value)))
    counters = indices.map(lambda x: initial_map(x, transactions, numOfProcesses)).collect()
    # import pdb; pdb.set_trace()
    # with Pool(processes=numOfProcesses) as pool:
        # counters = pool.map(initial_map, get_start_pos(len(transactions),int(len(transactions)/numOfProcesses)))
    for c in counters:
        web_page_counters += c[0]
        for page in c[1]:
            page_to_transactions[page] = page_to_transactions[page].union(c[1][page])

    total_broadcast_size += len(pickle.dumps(page_to_transactions))
    page_to_transactions = sc.broadcast(page_to_transactions)
    
    first_level_patterns = Counter({k: c for k, c in web_page_counters.items() if c >= min_rf.value*len(transactions.value) })
    
    logger.info("Got first level patterns : {}  writing to file".format(len(first_level_patterns)))
    
    save_first_level_patterns(os.path.join(folderPath,"1_level_patterns.txt"), first_level_patterns)
    
    logger.info("Generating second level candidateset")
    
    shuffle_write = 0
    shuffle_read = 0
    # exit(1)
    candidateset_size = 0
    coverage_patterns_size = 0
    
    if True:
        
        # even_level_patterns = manager.dict()
        odd_level_patterns = dict()
        even_level_patterns = getSecondLevelCandidateSet(max_or, max_cs, len(transactions.value), first_level_patterns)
        # getSecondLevelCandidateSet(first_level_patterns, even_level_patterns)
    
        level = 2
        
        logger.info("saving {} level patterns".format(level))
        save_nth_level_patterns(os.path.join(folderPath,"{}_level_patterns.txt".format(level)), even_level_patterns)
        level += 1
        
        while True:
            if level == 3:
                pass
            elif (level%2 == 0 and len(even_level_patterns) < 1) or (level%2==1 and len(odd_level_patterns) < 1):
                break
            logger.info("computing patterns for level : {}".format(level))

            if True:
                if level%2==0:
                    del even_level_patterns
                    # even_level_patterns.clear()
                    shuffle_read += len(pickle.dumps(odd_level_patterns))
                    pool = sc.parallelize(args_yielder(odd_level_patterns, transactions_len.value))
                    # results = pool.map(getNextLevelCandidateSetMapper).collect()
                    # even_level_patterns = []
                    # for r in results:
                    #     even_level_patterns.append(r[0])
                    #     candidateset_size += r[1]
                    #     coverage_patterns_size += r[2]
                    even_level_patterns = pool.map(getNextLevelCandidateSetMapper).collect()
                    shuffle_write += len(pickle.dumps(even_level_patterns))
                    # even_level_patterns = dict(ChainMap(*result))
                    # pool.map(getNextLevelCandidateSetMapper, args_yielder(odd_level_patterns, even_level_patterns))
                else:
                    del odd_level_patterns
                    # odd_level_patterns.clear()
                    shuffle_read += len(pickle.dumps(even_level_patterns))
                    pool = sc.parallelize(args_yielder(even_level_patterns, transactions_len.value))
                    # results = pool.map(getNextLevelCandidateSetMapper).collect()
                    odd_level_patterns = pool.map(getNextLevelCandidateSetMapper).collect()
                    # odd_level_patterns = []
                    # for r in results:
                        # odd_level_patterns.append(r[0])
                        # candidateset_size += r[1]
                        # coverage_patterns_size += r[2]
                    shuffle_write += len(pickle.dumps(odd_level_patterns))
                    # odd_level_patterns = dict(ChainMap(*result))
                    # import pdb; pdb.set_trace()
                    # pool.map(getNextLevelCandidateSetMapper, args_yielder(even_level_patterns, odd_level_patterns))
            logger.info("saving {} level patterns".format(level))
            if (level%2 == 0 and len(even_level_patterns) != 0) or (level%2==1 and len(odd_level_patterns) != 0):
                if level%2 == 0:
                    save_nth_level_patterns(os.path.join(folderPath,"{}_level_patterns.txt".format(level)), even_level_patterns)
                else:
                    save_nth_level_patterns(os.path.join(folderPath,"{}_level_patterns.txt".format(level)), odd_level_patterns)
            else:
                break
            level += 1
        print("candidate size: {}\n coverage patterns size: {}".format(candidateset_size, coverage_patterns_size))
        return total_broadcast_size, shuffle_read, shuffle_write



if __name__ == "__main__":
    logger.info("Got the following arguments: {}".format(sys.argv))
    numOfProcesses = int(sys.argv[4])
    APP_NAME = "Parallel-Cmine-Ultra-Pro-Max"
    conf = SparkConf().setAppName(APP_NAME).set('spark.default.parallelism',numOfProcesses).set("spark.executor.memory", "100g").set("spark.executor.memory	","100g").set("spark.python.worker.memory","100g").set("spark.driver.maxResultSize","100g")
    
	# conf = conf.setMaster("local[*]")
    # sc = SparkContext(conf=conf)
    sc = SparkContext.getOrCreate()
    min_rf = float(sys.argv[1])
    max_or = float(sys.argv[3])
    max_cs = float(sys.argv[2])
    # print(sc.getConf().getAll())
    transactions_file_path = str(sys.argv[5])
    min_rf = sc.broadcast(min_rf)
    max_cs = sc.broadcast(max_cs)
    max_or = sc.broadcast(max_or)
    t1 = time.time()
    total_broadcast_size, shuffle_read, shuffle_write = ParallelCmine(sc, transactions_file_path, min_rf, folderPath, numOfProcesses)
    t2 = time.time()
    logger.info("total time taken: {}".format(str(t2-t1)))
    with open("{}/metrics.txt".format(folderPath),'w') as f:
        logging.info("broadcast size:{}\nshuffle read:{}\nshuffle write:{}".format(total_broadcast_size, shuffle_read, shuffle_write))
        f.write("broadcast size:{}\nshuffle read:{}\nshuffle write:{}".format(total_broadcast_size, shuffle_read, shuffle_write))
    # while True:
    #     time.sleep(10)


# def firstLevelWorker(db_partition):
#     itd = {}
#     for transaction in db_partition:
#         for item in transaction:
#             itd[item].insert(i)
#     itd = filter(itd, lambda x: len(x)/len(DB) >= minRF)
#     return itd

# \begin{algorithm}[H]
# \caption{First iteration-Computing $CP_1$, $NOP_1$ ($DB$)}\label{algo:FirstIteration}%
#    \begin{algorithmic}
#         \Procedure{firstLevelWorker}{$dbPartition, minRF$}
#             \State $itd\gets \varnothing$
#             \ForAll{$\var{transaction_i}$ in $\var{dbPartition}$}:
#                 \ForAll{$\var{item}$ in $\var{transaction_i}$}:
#                     \State $itd[item]\gets itd[item] \bigcup {i} $
#                 \EndFor
#             \EndFor
#             \State $itd \gets \{ item \in \forall itd$ and $len(itd[item]) >= minRF \}$
#             \State \textbf{return} $itd$
#     \end{algorithmic}

#     \begin{algorithmic}
#         \Procedure{firstLevelMaster}{value=$DB$}
#             \State $dbPartitions \gets split(DB,N)$
#             \State $\var{itdCollect} \gets parallelize(firstLevelWorker, dbPartitions)$
#             \State $\var{itd} \gets merge(\var{itdCollect})$
#             \State $\var{nops} \gets \var{itd}$
#             \State $\var{cps} \gets \{item \in \forall itd$ and $len(itd[item]) >= minCS \}$
#     \end{algorithmic}
# \end{algorithm}


# def firstLevelMaster(DB):
#     db_partitions = split(DB, N)
#     itd_collect = parallilze(firstLevelWorker, db_partitions)
#     itd = merge(itd_collect)
#     nops = itd
#     cps = filter(nops, lambda x: len(x)/len(DB) >= minCS)


# def secondLevelWorker(pattern, maxOR):
#     coverage_set = itd[pattern] #As pattern is one-level pattern=item
#     CPS = {}
#     penultimate_prefix = pattern
#     for item in itd such that len(itd[item]) < len(itd[pattern]):
#         new_coverage_set = coverage_set U itd[item]
#         OR = len(coverage_set) + len(itd[item]) - len(new_coverage_set)
#         CS = len(new_coverage_set)
#         if OR < maxOR:
#             CPS[penultimate_prefix].insert(<item, OR, CS>)
#     return CPS

# \begin{algorithm}[H]
# \caption{Second iteration-Computing $CP_1$, $NOP_1$ ($DB$)}\label{algo:SecondIteration}%
#    \begin{algorithmic}
#         \Procedure{SLW}{$pattern, maxOR$}
#             \State $\var{cSet} \gets \var{itd[pattern]}$
#             \State $\var{CPS} \gets \varnothing$
#             \State $\var{PP} \gets \var{pattern}$
#             \ForAll{$\var{item}$ in $\var{itd} \& len(\var{itd[item]}) < len(\var{itd[pattern]})$}:
#                 \State $\var{newCSet} \gets \var{cSet} \bigcup \var{itd[item]}$
#                 \State $\var{OR} \gets len(\var{cSet}) + len(\var{itd[item]}) - len(\var{newCSet})$
#                 \State $\var{CS} \gets len(\var{newCSet})$
#                 \If{$OR < maxOR$}
#                     \State $\var{CPS[PP]} \gets \var{CPS[PP]} \bigcup {<item,OR,CS>} $    
#                 \EndIf
#             \EndFor
#             \State \textbf{return} $CPS$
#     \end{algorithmic}

#     \begin{algorithmic}
#         \Procedure{secondLevelMaster}{$firstLevelNOPs$}
#             \State $\var{nopsCollect} \gets parallelize(SLW, firstLevelNOPs)$
#             \State $\var{nops} \gets merge(nops_collect)$
#             \State $\var{cps} \gets \{<PP, item, OR, CS> \in \forall nops$ and $CS >= minCS \}$
#     \end{algorithmic}
# \end{algorithm}

# def secondLevelMaster(first_level_nops):
#     nops_collect = parallelize(secondLevelWorker, first_level_nops)
#     nops = merge(nops_collect)
#     cps = filter(nops, lambda x: x['cs'] >= minCS)




# def kthLevelWorker(CPS_set, maxOR):
#     coverage_set = {}
#     for item in penultimate_prefix:
#         coverage_set = coverage_set U itd[item]
#     CPS = {}
#     for <itemi, CSi, ORi> in CPS_set[penultimate_prefix]:
#         new_coverage_set = coverage_set U itd[itemi]
#         new_penultimate_prefix = penultimate_prefix U itemi
#         for <itemj, CSj, ORj> in CPS_set[penultimate_prefix]:
#             cur_coverage_set = new_coverage_set U itd[itemj]
#             cur_OR = len(cur_coverage_set) + len(itd[itemj]) - len(cur_coverage_set)
#             cur_CS = len(cur_coverage_set)
#             if OR < maxOR:
#                 CPS[new_penultimate_prefix].insert(<itemj, cur_OR, cur_CS>)
#     return CPS

# \begin{algorithm}[H]
# \caption{Second iteration-Computing $CP_1$, $NOP_1$ ($DB$)}\label{algo:SecondIteration}%
#    \begin{algorithmic}
#         \Procedure{KLW}{$CPS_set:<PP, lastItemList>, maxOR$}
#             \State $\var{cSet} \gets \varnothing}$
#             \ForAll{$\var{item}$ in $PP$}:
#                 \State $\var{cSet} \gets \var{cSet} \bigcup \var{itd[item]}$
#             \EndFor
#             \State $\var{CPS} \gets \varnothing$
#             \ForAll{$\<item_i,CS_i,OR_i>$ in $lastItemList$}:
#                 \State $\var{newCSet} \gets \var{cSet} \bigcup \var{itd[item_i]}$
#                 \State $\var{newPP} \gets \var{PP} \bigcup \var{item_i}$
#                 \ForAll{$\<item_j,CS_j,OR_j>$ in $lastItemList$}:
#                     \State $\var{curCSet} \gets \var{newCSet} \bigcup \var{itd[item_j]}$
#                     \State $\var{OR} \gets len(\var{newCSet}) + len(\var{itd[item_j]}) - len(\var{curCSet})$
#                     \State $\var{CS} \gets len(\var{curCSet})$
#                     \If{$OR < maxOR$}
#                         \State $\var{CPS[newPP]} \gets \var{CPS[newPP]} \bigcup {<item_j,OR_j,CS_j>} $    
#                     \EndIf
#                 \EndFor
#             \EndFor
#             \State \textbf{return} $CPS$
#     \end{algorithmic}

#     \begin{algorithmic}
#         \Procedure{kthLevelMaster}{$prevLevelNOPs$}
#             \State $\var{nopsCollect} \gets parallelize(KLW, prevLevelNOPs)$
#             \State $\var{nops} \gets merge(nops_collect)$
#             \State $\var{cps} \gets \{<PP, item, OR, CS> \in \forall nops$ and $CS >= minCS \}$
#     \end{algorithmic}
# \end{algorithm}

# def kthLevelMaster(second_level_nops):
#     nops_collect = parallelize(thirdLevelWorker,second_level_nops)
#     nops = merge(nops_collect)
#     cps = filter(nops, lambda x: x['cs'] >= minCS)

