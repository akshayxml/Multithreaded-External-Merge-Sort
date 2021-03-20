import os
import sys
from operator import itemgetter
import heapq
import threading
import time

start_time = time.time()

MEMORY_LIMIT = 0
THREAD_COUNT = 0
INPUT_FILE = 'input.txt'
OUTPUT_FILE = 'output.txt'
TUPLE_SIZE = 0
TOTAL_TUPLES = 0
TOTAL_FILESIZE = 0
TOTAL_SUBFILES = 0
TUPLES_PER_SUBFILE = 0
ROW_LEN = 0
TUPLES_PER_THREAD = 0

cols_to_sort = []
col_index_to_sort = []
thread_reqd = False
asc = False
col_details = dict()
file_counter = 0

class heap_object(object):
    def __init__(self, val, filename):
        self.val = val
        self.filename = filename
    def __lt__(self, other):
        for i in col_index_to_sort:
            if(asc == True):
                if(self.val[i] < other.val[i]):
                    return True
                elif(self.val[i] > other.val[i]):
                    return False
            else:
                if(self.val[i] > other.val[i]):
                    return True
                elif(self.val[i] < other.val[i]):
                    return False
        return False

def check_args(args):
    print('[CHECKING] Arguments')
    if(len(args) < 6):
        print('[ERROR] Invalid Argument Count')
        sys.exit(1)

    global MEMORY_LIMIT, thread_reqd, cols_to_sort, asc, THREAD_COUNT, INPUT_FILE, OUTPUT_FILE

    MEMORY_LIMIT = int(args[3])*1000*1000

    try:
        THREAD_COUNT = int(args[4])
        thread_reqd = True
    except:
        thread_reqd = False

    start = 4
    if(thread_reqd == True):
        start = 5
        
    asc = True if args[start].lower() == 'asc' else False
    for i in range(start+1, len(args)):
        cols_to_sort.append(args[i])

    INPUT_FILE = args[1]
    OUTPUT_FILE = args[2]
    
def read_metadata():
    print('[READING] Metadata')
    global TUPLE_SIZE, col_details

    with open('metadata.txt') as f:
        index = 0
        for line in f:
            line = line.split(',')
            size = int(line[1].rstrip())
            col_details[line[0]] = [index, size]
            TUPLE_SIZE += size
            index += 1
    f.close()

    for i in cols_to_sort:
        col_index_to_sort.append(col_details[i][0])

def set_details():
    global TOTAL_TUPLES, TOTAL_FILESIZE, TOTAL_SUBFILES, TUPLES_PER_SUBFILE, ROW_LEN

    with open(INPUT_FILE) as f:
        for i, l in enumerate(f):
            pass
    TOTAL_TUPLES = i + 1
    TOTAL_FILESIZE = TOTAL_TUPLES*TUPLE_SIZE
    TOTAL_SUBFILES = (TOTAL_FILESIZE+MEMORY_LIMIT-1)//MEMORY_LIMIT
    TUPLES_PER_SUBFILE = MEMORY_LIMIT//TUPLE_SIZE

    with open(INPUT_FILE, 'r') as f:
        ROW_LEN = len(f.readline())+1
    f.close()
    print('[DETAILS] Memory Limit = ' + str(MEMORY_LIMIT) + ' B')
    print('[DETAILS] Tuple Size = ' + str(TUPLE_SIZE) + ' B')
    print('[DETAILS] Total tuples = ' + str(TOTAL_TUPLES))
    if(thread_reqd == False):
        print('[DETAILS] Total File Size = ' + str(TOTAL_FILESIZE) + ' B')
    print('[DETAILS] Sub Files Required = ' + str(TOTAL_SUBFILES))

    # if(TOTAL_FILESIZE < MEMORY_LIMIT):
        # print('[ERROR] Two Phase Merge Sort is not required as memory limit is more than file size')
        # sys.exit(1)
    if(TOTAL_SUBFILES * TUPLE_SIZE > MEMORY_LIMIT):
        print('[ERROR] Memory Limit is less than what is required to hold the tuples during second phase')
        sys.exit(1)


def line_to_tuple(line):
    res = []
    for i in col_details.values():
        res.append(line[:i[1]])
        line = line[i[1]+2:]
    return res

def create_subfiles():
    print('[CREATING] Subfiles')
    count = 0
    filenum = 0
    f = open(str(filenum)+'.txt', 'w')
    f.close()

    with open(INPUT_FILE) as input:
        for line in input:
            if(count == 0):
                f = open(str(filenum)+'.txt', 'w')
                filenum += 1
            f.write(line)
            count += 1
            if(count == TUPLES_PER_SUBFILE):
                count = 0
                f.close()
                
        if(f.closed == False):
            f.close()

    input.close()

def sort_subfiles():
    for i in range(TOTAL_SUBFILES):
        cur = str(i)+'.txt'
        table= []
        with open(cur, 'r+') as f:
            print('[SORTING] Subfile #' + str(i))
            for line in f:
                row = line_to_tuple(line)
                table.append(row)

            if(asc == True):
                table.sort(key = itemgetter(*col_index_to_sort))
            else:
                table.sort(key = itemgetter(*col_index_to_sort), reverse=True)

            f.truncate(0)
            f.seek(0)
            print('[WRITING] Subfile #' + str(i))
            
            for row in table:
                for cell in range(len(row)):
                    f.write(row[cell])
                    if(cell != len(row)-1):
                        f.write('  ')
                    else:
                        f.write(' ')
                f.write('\n')
            f.close()

def thread_handler(start, end, filenum):
    print('[CREATING] Subfiles #' + str(filenum))
    count = 0
    f = open(str(filenum)+'.txt', 'w')
    start_read = False

    with open(INPUT_FILE) as input:
        for line in input:
            if(count == start):
                start_read = True
            if(count == end):
                f.write(line)
                f.close()
                break
            if(start_read):
                f.write(line)
            count += 1
                
        if(f.closed == False):
            f.close()

    input.close()

    table = []
    with open(str(filenum) + '.txt', 'r+') as f:
        print('[SORTING] Subfile #' + str(filenum))
        for line in f:
            row = line_to_tuple(line)
            table.append(row)

        if(asc == True):
            table.sort(key = itemgetter(*col_index_to_sort))
        else:
            table.sort(key = itemgetter(*col_index_to_sort), reverse=True)

        f.truncate(0)
        f.seek(0)
        print('[WRITING] Subfile #' + str(filenum))
        
        for row in table:
            for cell in range(len(row)):
                f.write(row[cell])
                if(cell != len(row)-1):
                    f.write('  ')
                else:
                    f.write(' ')
            f.write('\n')
        f.close()
            
def threaded_phase1():
    global TOTAL_SUBFILES, TUPLES_PER_THREAD, file_counter
    PARTITIONS = TOTAL_SUBFILES
    TOTAL_SUBFILES = TOTAL_SUBFILES * THREAD_COUNT
    TUPLES_PER_THREAD = TUPLES_PER_SUBFILE // THREAD_COUNT

    for i in range(PARTITIONS):
        threads = []
        for j in range(THREAD_COUNT):
            start = i*TUPLES_PER_SUBFILE + j*TUPLES_PER_THREAD
            if(start > TOTAL_TUPLES):
                TOTAL_SUBFILES = file_counter
                break
            end = i*TUPLES_PER_SUBFILE + (j+1)*TUPLES_PER_THREAD-1
            if(j==THREAD_COUNT-1):
                end = ((i+1)*TUPLES_PER_SUBFILE)-1
            if(end > TOTAL_TUPLES):
                TOTAL_SUBFILES = file_counter+1
                end = TOTAL_TUPLES

            threads.append(threading.Thread(target=thread_handler, args=(start, end, file_counter)))
            file_counter += 1

        if(TOTAL_SUBFILES * TUPLE_SIZE > MEMORY_LIMIT):
            print('[ERROR] Memory Limit is less than what is required to hold the tuples during second phase')
            sys.exit(1)

        for j in threads:
            j.start()
        for j in threads:
            j.join()

def phase1():
    print('[STARTED] Phase 1')

    if(thread_reqd == False):
        create_subfiles()
        sort_subfiles()
    else:
        threaded_phase1()
        
    print('[COMPLETED] Phase 1 after ' + str(time.time() - start_time) )

def phase2():
    print('[STARTED] Phase 2')
    filenames = [str(x)+'.txt' for x in range(TOTAL_SUBFILES)]
    fp = {filename: open(filename, 'r+') for filename in filenames}
    out = open(OUTPUT_FILE, 'w')

    heap = []

    for i in fp:
        line = fp[i].readline()
        if(len(line) == 0):
            fp[i].close()
            del fp[i]
        else:
            heapq.heappush(heap, heap_object(line_to_tuple(line), i))

    files_done = 0

    while(files_done < TOTAL_SUBFILES):
        obj = heapq.heappop(heap)
        row = obj.val
        filename = obj.filename

        for i in range(len(row)):
            out.write(row[i])
            if(i != len(row)-1):
                out.write('  ')
        out.write('\n')

        line = fp[filename].readline()
        if(len(line) == 0):
            files_done += 1
        else:
            heapq.heappush(heap, heap_object(line_to_tuple(line), filename))

    for i in fp:
        fp[i].close()
    out.close()

    print('[COMPLETED] Phase 2')

def del_subfiles():
    print('[DELETING] Subfiles')

    for i in range(TOTAL_SUBFILES):
        os.remove(os.getcwd()+'/'+str(i)+'.txt')

if __name__ == '__main__':
    
    check_args(sys.argv)
    read_metadata()
    set_details()
    phase1()
    phase2()
    del_subfiles() 

    print("--- %s seconds ---" % (time.time() - start_time))
