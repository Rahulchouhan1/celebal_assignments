# --------------------------------------------
# Question -> 9
# --------------------------------------------
# Problem Statement:
# You are given a dictionary that stores student names as keys and a list of their marks as values.
# Your task is to read the name of a student and print the average of their marks, rounded to 2 decimal places.

n = int(input("Enter number of students: "))
student_marks = {}
for _ in range(n):
    name, *line = input("Enter student name followed by marks: ").split()
    scores = list(map(float, line))
    student_marks[name] = scores

query_name = input("Enter the name of the student to query: ")
avg = sum(student_marks[query_name]) / len(student_marks[query_name])
print(f"Average marks of {query_name}: {avg:.2f}")