import os
import random
import math
import bisect as bs
import time

from queue import PriorityQueue


pq = PriorityQueue()
k = 500  # Top K
bucket_size = 1000  # Bucket Size
run_size = 5000  # Size of Each Run
num_partition = 0
hashmap = dict()
start_time = 0
num_rows = 0
num_runs = 0

def run_generation():
    global num_partition
    # f = open("SecondStorage.txt", "w")
    inp = [i for i in range(1000000)]
    random.shuffle(inp)
    # mock input
    query = [9, 70, 2, 5, 50, 52, 12, 5, 200, 10, 3, 170, 1, 8]
    # random.shuffle(input)
    totalBsize = 0
    chunk = []
    cutoff = None
    count = 0
    for n in inp:
        # print("The "+str(count)+":"+str(n)+", KEY:"+str(cutoff))
        # Only filtering input when cutoff key has been defined
        if cutoff is not None:
            if n > cutoff: continue  # Filter the input

        # keep adding input to the bucket
        if len(chunk) < run_size:
            bs.insort_left(chunk, n)

        # After the insertion, if the bucket size is full and need to perform a insertion to the pq
        if len(chunk) == run_size:
            chunk.reverse()
            # tmpK = bucket[0] # Since the bucket is sorted, the Min/Max number is at head
            # pq.put((-tmpK, bucket.copy())) # Since it is a Min Heap, we can insert model a max heap by * -1
            buckets = list(split_bucket(chunk))
            # print(chunks)
            totalBsize, cutoff, writeout = insert2queue(buckets, totalBsize, cutoff)
            writeout.reverse()  # Reverse Back
            f = open("tmp/out" + str(count), "w")
            num_partition += 1
            count += 1
            for item in writeout:
                f.write("%s\n" % item)
            # f.write(str(writeout).strip('[]') + "\n")  # write to second storage place
            f.close()
            chunk.clear()  # clear the bucket for the later runs
            print("Cutoff Key: " + str(cutoff))
            # print(pq.queue)
            print("--------------------------")
        # print(pq.queue)
    if len(chunk) > 0:  # Remaining items in bucket
        chunk.reverse()
        # print(bucket)
        buckets = list(split_bucket(chunk))
        # print(chunks)
        _, _, writeout = insert2queue(buckets, totalBsize, cutoff)
        writeout.reverse()
        f = open("tmp/out" + str(count), "w")
        num_partition += 1
        # f.write(str(writeout).strip('[]') + "\n")  # write to second storage place
        count += 1
        for item in writeout:
            f.write("%s\n" % item)
        # f.write(str(writeout).strip('[]') + "\n")  # write to second storage place
        f.close()
    # f.close()


def run_generation_spatial():
    global num_partition
    # f = open("SecondStorage.txt", "w")
    target = [0, 0]
    inp = [[i, j] for i in range(1000) for j in range(1000)]
    random.shuffle(inp)
    # random.shuffle(input)

    global start_time 
    global num_rows
    global num_runs
    start_time = time.clock()

    totalBsize = 0
    chunk = []
    cutoff = None
    count = 0
    
    for (i, j) in inp:
        p = [i, j]
        n = math.sqrt(((p[0] - target[0]) ** 2) + ((p[1] - target[1]) ** 2))

        n = round(n, 4)
        # print("The "+str(count)+":"+str(n)+", KEY:"+str(cutoff))
        # Only filtering input when cutoff key has been defined
        if cutoff is not None:
            if n > cutoff: continue  # Filter the input
        num_rows += 1
        # Insert it into hashmap
        if n not in hashmap:
            hashmap[n] = [p]
        else:
            hashmap[n].append(p)
        # keep adding input to the bucket
        if len(chunk) < run_size:
            bs.insort_left(chunk, n)

        # After the insertion, if the bucket size is full and need to perform a insertion to the pq
        if len(chunk) == run_size:
            chunk.reverse()
            # tmpK = bucket[0] # Since the bucket is sorted, the Min/Max number is at head
            # pq.put((-tmpK, bucket.copy())) # Since it is a Min Heap, we can insert model a max heap by * -1
            buckets = list(split_bucket(chunk))
            # print(chunks)
            totalBsize, cutoff, writeout = insert2queue(buckets, totalBsize, cutoff)
            writeout.reverse()  # Reverse Back
            f = open("tmp/out" + str(count), "w")
            num_partition += 1
            count += 1
            for item in writeout:
                f.write("%s\n" % item)
            # f.write(str(writeout).strip('[]') + "\n")  # write to second storage place
            f.close()
            num_runs += 1
            chunk.clear()  # clear the bucket for the later runs
            #print("Cutoff Key: " + str(cutoff))
            # print(pq.queue)
          #  print("--------------------------")
        # print(pq.queue)
    if len(chunk) > 0:  # Remaining items in bucket
        chunk.reverse()
        # print(bucket)
        buckets = list(split_bucket(chunk))
        # print(chunks)
        _, _, writeout = insert2queue(buckets, totalBsize, cutoff)
        writeout.reverse()
        f = open("tmp/out" + str(count), "w")
        num_partition += 1
        # f.write(str(writeout).strip('[]') + "\n")  # write to second storage place
        count += 1
        for item in writeout:
            f.write("%s\n" % item)
        # f.write(str(writeout).strip('[]') + "\n")  # write to second storage place
        f.close()
        num_runs += 1
    # f.close()
    print("Number of rows:"+str(num_rows))
    print("Number of runs:"+str(num_runs))

def split_bucket(bucket):
    for i in range(0, len(bucket), bucket_size):
        yield bucket[i:i + bucket_size]


def insert2queue(chunks, totalBsize, cutoff):
    outputChunk = []
    for x in chunks:
        totalBsize += len(x)
        x_key = x[0]  # Since the bucket is sorted, the Min/Max number is at head
        if (cutoff is not None and x > pq.queue[0][1]): continue  # Corner Case
        pq.put((-x_key, x.copy()))  # Since it is a Min Heap, we can insert model a max heap by * -1
        outputChunk.extend(x)
        if totalBsize >= k:
            if cutoff is None:  # First time we got a cutoff key
                cutoff = -pq.queue[0][0]
            else:  # Not first time, need to perform a queue.poll and refine new key
                pq.get()  # Pop
                cutoff = -pq.queue[0][0]
    return totalBsize, cutoff, outputChunk


def clean_files():
    path, dirs, files = next(os.walk("tmp"))
    file_count = len(files)

    for i in range(file_count):
        file_name = "tmp/out" + str(i)
        if os.path.exists(file_name):
            os.remove(file_name)


if __name__ == '__main__':
    clean_files()
    run_generation_spatial()
    print(hashmap)
    # inp = [[i, j] for i in range(10) for j in range(8)]
    # target = [0, 0]
    # for (i, j) in inp:
    #     p = [i, j]
    #     n = math.sqrt(((p[0] - target[0]) ** 2) + ((p[1] - target[1]) ** 2))
    #     n = round(n, 3)
    #     print(p)
    #     print(n)
    # run_generation()
    # print(pq.queue)
    # print("-----------")
    # main()
    # q = PriorityQueue()
    # q.put(10)
    # q.put(1)
    # q.put(90)
    # q.put(80)
    # q.put(0)
    # q.put(1000)
    # print(q.queue)
    # q.get()
    # print(q.queue)
    # while not pq.empty():
    #     next_item = pq.get()
    #     print(next_item)
