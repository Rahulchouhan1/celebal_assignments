# --------------------------------------------
# Question -> 7
# --------------------------------------------
# Problem Statement:
# You are given a list of lowercase English letters. You can randomly select any k letters (without replacement).
# Your task is to calculate the probability that at least one of the selected letters is 'a'.

from itertools import combinations

n = int(input("Enter number of elements in the list: "))
letters = input("Enter the letters separated by space: ").split()
k = int(input("Enter number of selections (k): "))

all_combinations = list(combinations(letters, k))
count_of_a = sum(1 for comb in all_combinations if 'a' in comb)
probability = count_of_a / len(all_combinations)
print(f"Probability: {probability:.4f}")