# Question -> 4

import string

def print_rangoli(size):
    alphabets = string.ascii_lowercase
    lines = []

    for i in range(size):
        left = alphabets[size-1:i:-1]
        center = alphabets[i]
        right = alphabets[i+1:size]
        line = '-'.join(left + center + right)
        lines.append(line.center(4*size - 3, '-'))

    print('\n'.join(lines[::-1] + lines[1:]))

if __name__ == '__main__':
    n = int(input())
    print_rangoli(n)