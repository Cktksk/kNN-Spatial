import ex_merge
import run_generator
import sys
import os
import time

top_k = 0
seek = dict()
try:
    top_k = int(sys.argv[1])
    run_generator.k = top_k
except:
    print("usage: python ex_merge.py top_k")
    exit()

# Removing Previous Files
if not os.path.exists("tmp"):
    os.mkdir("tmp")

path, dirs, files = next(os.walk("tmp"))
file_count = len(files)

for i in range(file_count):
    file_name = "tmp/out" + str(i)
    if os.path.exists(file_name):
        os.remove(file_name)
print("Executing Driver")
#run_generator.run_generation()
run_generator.run_generation_spatial()
ex_merge.merge(top_k, run_generator.num_partition)

file1 = open('output', 'r')
Lines = file1.readlines()
out = open('processed_points', 'w')
# Strips the newline character  
# print(run_generator.hashmap)
for line in Lines:
    line = line.strip('\n')
    if line not in seek:
        seek[line] = 0
    else:
        seek[line] += 1
    out.write(line + ': '+str(run_generator.hashmap.get(float(line))[seek.get(line)]) + '\n')
print("passed time(after generating input): " + str(time.perf_counter() - run_generator.start_time) +' seconds\n')
