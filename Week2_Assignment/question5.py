#question 5 :-> 
def merge_the_tools(string, k):
    my_list = [string[i:i+k] for i in range(0,len(string),k)]
    for substring in my_list:
        unique_element = ""
        for char in substring:
            if char not in unique_element:
                unique_element+=char
        print(unique_element)
    
if __name__ == '__main__':
    string, k = input(), int(input())
    merge_the_tools(string, k)