# --------------------------------------------
# Question -> 8
# --------------------------------------------
# Problem Statement:
# Given an integer n and n space-separated integers, create a tuple t of those n integers.
# Then compute and print the result of hash(t).

n = int(input("Enter number of elements for the tuple: "))
integer_list = map(int, input("Enter the integers: ").split())
tup = tuple(integer_list)
print("Hash of the tuple:", hash(tup))