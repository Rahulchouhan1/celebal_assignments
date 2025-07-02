# question 2 : solution for hackerrank

def average(array):
    my_set = set(array)
    sum_of_ele = 0
    for ele in my_set:
        sum_of_ele+=ele
    return sum_of_ele/len(my_set)
if __name__ == '__main__':
    n = int(input())
    arr = list(map(int, input().split()))
    result = average(arr)
    print(result)
    
