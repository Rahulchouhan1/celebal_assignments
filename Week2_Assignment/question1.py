# Question 1 :-> 



import math
import os
import random
import re
import sys

# Complete the solve function below. - > solution run only in hackerrank 
def solve(s):
   return ' '.join([word.capitalize() for word in s.split(' ')])
     
if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    s = input()

    result = solve(s)

    fptr.write(result + '\n')

    fptr.close()
