# --------------------------------------------
# Question -> 5
# --------------------------------------------
# Problem Statement:
# Write a function is_leap(year) that takes a year as input and returns:
# - True if the year is a leap year
# - False otherwise

def is_leap(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

year = int(input("Enter a year to check for leap year: "))
print("Leap Year:" if is_leap(year) else "Not a Leap Year")