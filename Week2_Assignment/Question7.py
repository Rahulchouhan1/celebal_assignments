# Question 7 :-> 

def main():
    n = int(input())
    for _ in range(n):
        try:
            line = input().split()
            a = int(line[0])
            b = int(line[1])
            print(a // b)
        except ZeroDivisionError as e:
            print(f"Error Code: {e}")
        except ValueError as e:
            print(f"Error Code: {e}")

if __name__ == "__main__":
    main()