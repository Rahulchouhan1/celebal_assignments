# --------------------------------------------
# Question -> 4
# --------------------------------------------
# Problem Statement: The Minion Game
# Kevin and Stuart are playing a game with a string.
# - Stuart can only make words starting with consonants.
# - Kevin can only make words starting with vowels.
# - The score of each player is the total number of substrings they can form that start with their allowed letters.
# The player with the highest score wins.

def minion_game(string):
    stuart_score = 0
    kevin_score = 0
    vowels = 'AEIOU'
    length = len(string)

    for i in range(length):
        if string[i] in vowels:
            kevin_score += length - i
        else:
            stuart_score += length - i

    if stuart_score > kevin_score:
        print("Stuart", stuart_score)
    elif kevin_score > stuart_score:
        print("Kevin", kevin_score)
    else:
        print("Draw")

s = input("Enter a string for Minion Game: ")
minion_game(s.upper())