# --------------------------------------------
# Question -> 10
# --------------------------------------------
# Problem Statement:
# Given an integer n, print the following values for each integer i from 1 to n:
# Decimal, Octal, Hexadecimal (uppercase), and Binary.
# All four values must be printed on a single line, separated by a single space.
# Each column should be right-aligned, and the width should be equal to the width of the binary representation of n.

def print_formatted(number):
    width = len(bin(number)[2:])
    for i in range(1, number + 1):
        dec = str(i).rjust(width)
        octal = oct(i)[2:].rjust(width)
        hexa = hex(i)[2:].upper().rjust(width)
        binary = bin(i)[2:].rjust(width)
        print(f"{dec} {octal} {hexa} {binary}")

n = int(input("Enter a number to format output: "))
print_formatted(n)