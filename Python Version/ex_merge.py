import random
import sys 
from typing import List, Optional 
import os

class MinHeapNode: 
    def __init__(self, el, i): 
        self.element = el # the element to be sorted 
        self.i = i         # index of array from which element is taken 
  
  
class MinHeap: 
    def __init__(self, ar: List[MinHeapNode], size: int): 
        self.heap_size = size 
        self.heap_arr = ar 
        i = (self.heap_size - 1) // 2
        while i >= 0: 
            self.min_heapify(i) 
            i -= 1
    def min_heapify(self, i): 
        l = left(i) 
        r = right(i) 
        smallest = i 
        if l < self.heap_size and self.heap_arr[l].element < self.heap_arr[i].element: 
            smallest = l 
        if r < self.heap_size and self.heap_arr[r].element < self.heap_arr[smallest].element: 
            smallest = r 
        if smallest != i: 
            swap(self.heap_arr, i, smallest) 
            self.min_heapify(smallest) 
  
    def get_min(self) -> Optional: 
        if self.heap_size <= 0: 
            print('Heap underflow') 
            return None
        return self.heap_arr[0] 
  
    # Replace root with new root 
    def replace_min(self, root): 
        self.heap_arr[0] = root 
        self.min_heapify(0) 
  
  
def left(i): 
    return 2 * i + 1
  
  
def right(i): 
    return 2 * i + 2
  
  
def swap(arr: List[MinHeapNode], i, j): 
    temp = arr[i] 
    arr[i] = arr[j] 
    arr[j] = temp 



# def createInitialRuns(run_size):
#     f = open('input', 'r')
#     #global num_partitions
#     count = 0
#     l = []
#     for line in f:
#         for i in line.split():
#             if i.isdigit():
#                 l.append(int(i))
#                 count += 1
#                 if count % run_size == 0:
#                     l.sort() # use python's tim sort in the run
#                     file = open('tmp/out' + str(num_partitions), 'w') # out0 out1 out2
#                     for item in l:
#                          file.write("%s\n" % item)
#                     file.close()
#                     l = []
#                     num_partitions += 1
#     f.close()

def merge(top_k, num_partitions):
    #global num_partitions
    print("Top K: "+str(top_k))
    k = num_partitions
    h_arr = [] 
    fout = open('output', 'w')  
    myFiles = [open('tmp/out'+ str(i), 'r') for i in range(k)]
    for i in range(k): 
        line = myFiles[i].readline()
        node = MinHeapNode(float(line), i)
        h_arr.append(node) 
    min_heap = MinHeap(h_arr, k) 
    count = 0
    curr_index = 0
    current_k = 0
    while count < k:
        root = min_heap.get_min()
        curr_index = root.i
        fout.write("%s\n" % root.element)
        current_k += 1
        if current_k == top_k:
            break
        next_num = myFiles[curr_index].readline()
        if next_num != '':
            root.element = float(next_num)
            root.i = curr_index
        else: 
            count += 1
            root.element = sys.maxsize 
        min_heap.replace_min(root) 
    for i in range(k):
        myFiles[i].close()
    fout.close()
    print("finished.")


# for i in range(100):
#     file_name = "tmp/out" + str(i)
#     if os.path.exists(file_name):
#         os.remove(file_name)
# a = list(range(1,1000001))  # 1~ 10000
# random.shuffle(a)  # generate a random permutation

#global top_k
# try:
#     top_k = int(sys.argv[1])
# except:
#     print("usage: python ex_merge.py top_k")
#     exit()

# f = open('input', 'w')
# for item in a:
#     f.write("%s\n" % item)
# f.close()
# run_size = 50000
#print("top_k: ", top_k)
#num_partitions = 47
#createInitialRuns(run_size)
#merge(top_k)
