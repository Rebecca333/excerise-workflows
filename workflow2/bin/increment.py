# increment number

import sys

try:
    output_file = sys.argv[2]
except:
    print('need more arguments')
    raise

file_name = sys.argv[1]

input_file = open(file_name, 'r')

for line in input_file:
    num = int(line)

num = num + 1

with open(output_file, 'w') as f:
    f.write(str(num))
f.close()
