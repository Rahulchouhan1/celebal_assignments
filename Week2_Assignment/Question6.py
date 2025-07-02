# Question 6 -> 

from collections import Counter
x = int(input())
my_list = list(map(int,input().split()))
Counter_list = Counter(my_list)
n = int(input())
total_money = 0
for _ in range(n):
    size,money = map(int,input().split())
    if Counter_list[size]>0:
        total_money+=money
        Counter_list[size]-=1
print(total_money)
