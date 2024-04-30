from multiprocessing import Value, Pool, Lock, Manager, Process
from collections import Counter, defaultdict, ChainMap
from itertools import combinations
import time, sys
import logging
import os
from pathlib import Path
from pyspark import SparkConf, SparkContext

Path(sys.argv[6]).mkdir(parents=True, exist_ok=True)

# transactions = None
# numOfProcesses = None
# candidate_set = None
# page_to_transactions = None
# max_or = None
# max_cs = None

folderPath = sys.argv[6]

logging.basicConfig(
    level=logging.DEBUG,
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
    return [line.strip().split(',') for line in lines]

def initial_map(start_pos):
    # global numOfProcesses
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
    global page_to_transactions
    global max_or
    global max_cs
    global transactions
    intersection_len = get_intersection_len(set1, set2)
    union_len = len(set1) + len(set2) - intersection_len
    return union_len/len(transactions), intersection_len/len(set2) 

def getSecondLevelCandidateSet(first_level_patterns, second_level_patterns):
    global page_to_transactions
    global max_or
    global max_cs
    global transactions
    pages = first_level_patterns.most_common()
    # second_level_patterns = defaultdict(list)
    # import pdb; pdb.set_trace()
    for i in range(0, len(pages)):
        # second_level_patterns[tuple([pages[i][0]])] = []#pages[j][0] for j in range(i+1, len(pages)) if check_conditions(page_to_transactions[pages[i][0]], page_to_transactions[pages[j][0]]) ]
        new_index = tuple([pages[i][0]])
        new_list = []
        for j in range(i+1,len(pages)):
            intersection_len = get_intersection_len(page_to_transactions[pages[i][0]],page_to_transactions[pages[j][0]])
            union_len = len(page_to_transactions[pages[i][0]]) + len(page_to_transactions[pages[j][0]]) - intersection_len
            cur_or = intersection_len/len(page_to_transactions[pages[j][0]])
            cur_cs = union_len/len(transactions)
            # print("check2", cur_or, cur_cs, max_or, new_index, pages[j][0])
            # if pages[j][0] == '684' or pages[j][0] == '766':
                # print("check", cur_or, cur_cs, max_or, new_index, pages[j][0])
            if cur_or <= max_or:
                new_list.append({
                    'page':pages[j][0],
                    'overlap':cur_or,
                    'coverage':cur_cs
                })
        second_level_patterns[new_index] = new_list

# (a,b,c), (a,b,d), (b,c,d)

# {('a','b'):[{'page':'c','overlap':x,'coverage':y}, {'page':'d','overlap':x,'coverage':y}, ('b','c'): [{'page':'d','overlap':x,'coverage':y}]]}

def getNextLevelCandidateSet(same_part_key, same_part_key_value, new_patterns):
    global page_to_transactions
    global transactions
    global max_or
    global max_cs
    global cur_level_patterns
    
    # for same_part_key in prev_level_patterns:
    # print(same_part_key,"started")
    if True:
        cur_set = set().union(*[page_to_transactions[same_part_key[i]] for i in range(0,len(same_part_key))])
        for i in range(len(same_part_key_value)):
            ith_page = same_part_key_value[i]['page']
            set_with_i = cur_set.union(page_to_transactions[ith_page])
            new_same_part_key = same_part_key + tuple([ith_page])
            cur_new_patterns = []
            for j in range(i+1,len(same_part_key_value)):
                new_page = same_part_key_value[j]['page']
                intersection_len = get_intersection_len(set_with_i, page_to_transactions[new_page])
                union_len = len(set_with_i) + len(page_to_transactions[new_page]) - intersection_len
                cur_or = intersection_len/len(page_to_transactions[new_page])
                cur_cs = union_len/len(transactions)
                
                if cur_or <= max_or:
                    cur_new_patterns.append({
                        'page':new_page,
                        'overlap':cur_or,
                        'coverage':cur_cs,
                        'max_or':max_or
                    })
            if len(cur_new_patterns) != 0:
                new_patterns[new_same_part_key] = cur_new_patterns
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
    return
    with open(file_path,'w') as f:
        for p in first_level_patterns:
            logger.info(p)
    #         f.write("{} | {} | {}\n".format(p,first_level_patterns[p],0))
            
def save_nth_level_patterns(file_path, patterns):
    # return
    count = sum([len(patterns[same_key]) for same_key in patterns])
    logger.info("found {} patterns".format(count))
    return
    with open(file_path,'w') as f:
        for same_key in patterns:
    #         # print(same_key, patterns[same_key])
            logger.info(patterns[same_key])
            for pattern in patterns[same_key]:
                logger.info("{} | {}".format(same_key, pattern))
    #             f.write("{}, {} | {} | {}\n".format(', '.join(map(str,same_key)), pattern['page'], pattern['coverage'], pattern['overlap']))


def args_yielder(cur_level_patterns, new_patterns):
    for same_part_key in cur_level_patterns:
        yield same_part_key, cur_level_patterns[same_part_key], new_patterns

def ParallelCmine(sc, inputFile, min_rf, folderPath):
    global transactions
    global numOfProcesses
    global candidate_set
    global page_to_transactions
    
    logger.info("started mining")
    
    logger.info("reading transactional data")
    # transactions = get_transactions_data(inputFile)
    transactions = sc.textFile(inputFile).map(lambda x:x.strip().encode("ascii", "ignore").split(','))

# transactions = None
# numOfProcesses = None
# # candidate_set = None
# page_to_transactions = None
# max_or = None
# max_cs = None
    logger.info("First level mining")

    transactions = sc.broadcast(transactions)
    numOfProcesses = sc.broadcast(numOfProcesses)
    max_or = sc.broadcast(max_or)
    max_cs = sc.broadcast(max_cs)
    
    web_page_counters = Counter()
    page_to_transactions = defaultdict(set)

    indices = sc.parallelize(get_start_pos(len(transactions),int(len(transactions)/numOfProcesses)))
    counters = indices.map(initial_map)

    # with Pool(processes=numOfProcesses) as pool:
        # counters = pool.map(initial_map, get_start_pos(len(transactions),int(len(transactions)/numOfProcesses)))
    for c in counters:
        web_page_counters += c[0]
        for page in c[1]:
            page_to_transactions[page] = page_to_transactions[page].union(c[1][page])
    
    first_level_patterns = Counter({k: c for k, c in web_page_counters.items() if c >= min_rf*len(transactions) and c <= max_cs*len(transactions) })
    
    logger.info("Got first level patterns : {}  writing to file".format(len(first_level_patterns)))
    
    save_first_level_patterns(os.path.join(folderPath,"1_level_patterns.txt"), first_level_patterns)
    
    logger.info("Generating second level candidateset")
    
    
    with Manager() as manager:
        
        even_level_patterns = manager.dict()
        odd_level_patterns = manager.dict()
        getSecondLevelCandidateSet(first_level_patterns, even_level_patterns)
    
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

            with Pool(processes=numOfProcesses) as pool:
                if level%2==0:
                    even_level_patterns.clear()
                    pool.map(getNextLevelCandidateSetMapper, args_yielder(odd_level_patterns, even_level_patterns))
                else:
                    odd_level_patterns.clear()
                    pool.map(getNextLevelCandidateSetMapper, args_yielder(even_level_patterns, odd_level_patterns))
            logger.info("saving {} level patterns".format(level))
            if (level%2 == 0 and len(even_level_patterns) != 0) or (level%2==1 and len(odd_level_patterns) != 0):
                if level%2 == 0:
                    save_nth_level_patterns(os.path.join(folderPath,"{}_level_patterns.txt".format(level)), even_level_patterns)
                else:
                    save_nth_level_patterns(os.path.join(folderPath,"{}_level_patterns.txt".format(level)), odd_level_patterns)
            else:
                break
            level += 1



if __name__ == "__main__":
	logger.info("Got the following arguments: {}".format(sys.argv))
    APP_NAME = "FINAL_DCPM"

	conf = SparkConf().setAppName(APP_NAME)
	# conf = conf.setMaster("local[*]")

	sc = SparkContext(conf=conf)

	min_rf = float(sys.argv[1])
	max_or = float(sys.argv[3])
	max_cs = float(sys.argv[2])
	numOfProcesses = int(sys.argv[4])
	transactions_file_path = str(sys.argv[5])
    
    min_rf = sc.broadcast(min_rf)
	max_cs = sc.broadcast(max_cs)
	max_or = sc.broadcast(max_or)

	t1 = time.time()
	output = ParallelCmine(sc, transactions_file_path, min_rf, folderPath)
	t2 = time.time()
	logger.info("total time taken: {}".format(str(t2-t1)))
