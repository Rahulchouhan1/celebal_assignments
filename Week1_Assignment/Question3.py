# --------------------------------------------
# Question -> 3
# --------------------------------------------
# Problem Statement:
# You are given a string S consisting of lowercase or uppercase letters and/or digits. Your task is to compress
# the string by replacing consecutive occurrences of the same character with a tuple that contains:
# - The count of consecutive occurrences
# - The character itself
# Use the groupby() function from the itertools module to achieve this.

from itertools import groupby

s = input("Enter a string to compress: ")
compressed = [(len(list(group)), key) for key, group in groupby(s)]
print("Compressed output:", ' '.join(str(item) for item in compressed))