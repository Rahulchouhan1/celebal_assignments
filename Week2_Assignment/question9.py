# Question 9 ;-> 

n = int(input())
s = set(map(int, input().split()))
m = int(input())
for _ in range(m):
    command =(input().split())
    if(command[0] == "pop"):
        s.pop()
    elif(command[0] == "remove"):
        s.remove(int(command[1]))
    else:
        s.discard(int(command[1]))
print(sum(s))
        