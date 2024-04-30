from multiprocessing import Value, Pool, Lock
from collections import Counter, defaultdict, ChainMap
from itertools import combinations
import time, sys
import logging
import os
from pathlib import Path

Path(sys.argv[6]).mkdir(parents=True, exist_ok=True)

transactions = None
numOfProcesses = None
candidate_set = None
page_to_transactions = None
max_or = None
max_cs = None

folderPath = sys.argv[6]

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(folderPath,"mine.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_transactions_data(file_path):
    lines = open(file_path,'r').read().splitlines()
    return [line.strip().split() for line in lines]

def initial_map(start_pos):
    global numOfProcesses
    end_pos = start_pos + min(int(len(transactions)/numOfProcesses),len(transactions)-start_pos)
    counter = Counter()
    cur_page_to_transactions = defaultdict(set)
    for i in range(start_pos, end_pos):
        counter.update(transactions[i])
        for p in transactions[i]:
            cur_page_to_transactions[p].add(i)
    return counter, cur_page_to_transactions

def getSecondLevelCandidateSet(first_level_patterns):
    pages = first_level_patterns.most_common()
    return [(pages[i][0], pages[j][0]) for i in range(0,len(pages)) for j in range(i+1,len(pages))]

def getNextLevelCandidateSet(prev_level_patterns):
    new_patterns = list()
    same_part_map = defaultdict(list) # {'a':['b','c','d']}
    for pattern in prev_level_patterns:
        same_part_map[pattern[:-1]].append(pattern[-1])
    for same_part_key in same_part_map:
        new_patterns += [ same_part_key + new_part for new_part in combinations(same_part_map[same_part_key],2) ]
    return new_patterns

def get_intersection_len(set1, set2):
    return sum(1 for i in set2 if i in set1)

def get_no_patterns(start_pos):
    global numOfProcesses
    global page_to_transactions
    global max_or
    global transactions
    end_pos = start_pos + min(int(len(candidate_set)/numOfProcesses),len(candidate_set)-start_pos)
    no_patterns = {}
    for i in range(start_pos, end_pos):
        cur_set = set().union(*[page_to_transactions[candidate_set[i][j]] for j in range(0,len(candidate_set[i]) -1)])
        intersection_len = get_intersection_len(cur_set, page_to_transactions[candidate_set[i][-1]])
        logger.debug("intersection length of {} and {}  = {}".format([candidate_set[i][j] for j in range(0,len(candidate_set[i]) -1)],  candidate_set[i][-1],intersection_len))
        logger.debug("{} {}".format(cur_set, page_to_transactions[candidate_set[i][-1]] ))
        union_len = len(cur_set) + len(page_to_transactions[candidate_set[i][-1]]) - intersection_len
        if intersection_len/len(page_to_transactions[candidate_set[i][-1]]) <= max_or and union_len/len(transactions) <= max_cs:
        	no_patterns[candidate_set[i]] = [union_len, intersection_len]
    return no_patterns
        
def get_start_pos(count, step):
    return list(range(0, count, step))

def save_first_level_patterns(file_path, first_level_patterns):
    with open(file_path,'w') as f:
        for p in first_level_patterns:
            f.write("{} | {} | {}\n".format(p,first_level_patterns[p],0))
            
def save_nth_level_patterns(file_path, patterns):
    with open(file_path,'w') as f:
        for p in patterns:
            f.write("{} | {} | {}\n".format(', '.join(map(str,p)), patterns[p][0], patterns[p][1]))

def ParallelCmine(inputFile, min_rf, folderPath):
    global transactions
    global numOfProcesses
    global candidate_set
    global page_to_transactions
    
    logger.info("started mining")
    
    logger.info("reading transactional data")
    transactions = get_transactions_data(inputFile)
    
    logger.info("First level mining")
    
    web_page_counters = Counter()
    page_to_transactions = defaultdict(set)
    with Pool(processes=numOfProcesses) as pool:
        counters = pool.map(initial_map, get_start_pos(len(transactions),int(len(transactions)/numOfProcesses)))
        for c in counters:
            web_page_counters += c[0]
            for page in c[1]:
                page_to_transactions[page] = page_to_transactions[page].union(c[1][page])
    
    first_level_patterns = Counter({k: c for k, c in web_page_counters.items() if c >= min_rf*len(transactions) and c <= max_cs*len(transactions) })
    
    logger.info("Got first level patterns : {}  writing to file".format(len(first_level_patterns)))
    
    save_first_level_patterns(os.path.join(folderPath,"1_level_patterns.txt"), first_level_patterns)
    
    logger.info("Generating second level candidateset")
    
    candidate_set = getSecondLevelCandidateSet(first_level_patterns)
    
    level = 2
    
    while True:
        if len(candidate_set) <= 1:
            break
        
        logger.info("computing non-overlap patterns for level : {}".format(level))
        
        no_patterns = {}
        with Pool(processes=numOfProcesses) as pool:
            no_patterns_parts = pool.map(get_no_patterns, get_start_pos(len(candidate_set), int(len(candidate_set)/numOfProcesses)))
            no_patterns_parts.reverse()
            no_patterns = dict(ChainMap(*no_patterns_parts))
        
        logger.info("saving {} level patterns, got : {}".format(level, len(no_patterns)))
        save_nth_level_patterns(os.path.join(folderPath,"{}_level_patterns.txt".format(level)), no_patterns)
        
        logger.info("computing next level candidates")
        new_patterns = no_patterns.keys()
        candidate_set = getNextLevelCandidateSet(new_patterns)
        level += 1



if __name__ == "__main__":
	logger.info("Got the following arguments: {}".format(sys.argv))
	min_rf = float(sys.argv[1])
	max_or = float(sys.argv[2])
	max_cs = float(sys.argv[3])
	numOfProcesses = int(sys.argv[4])
	transactions_file_path = str(sys.argv[5])
	t1 = time.time()
	output = ParallelCmine(transactions_file_path, min_rf, folderPath)
	t2 = time.time()
	logger.info("total time taken: {}".format(str(t2-t1)))
