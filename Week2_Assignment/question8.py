# Question 8 :-> 

import re

testcase = int(input())  

for _ in range(testcase):
    s = input()  
    
    try:
        
        re.compile(s)  
        print("True")  
    except re.error:
      
        print("False")  