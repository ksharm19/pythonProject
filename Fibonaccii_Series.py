#Fibonacii series

num = int(input("Enter the number: "))

n1 = 0
n2 = 1
sum = 0

if num < 0:
    print ("Please enter the number greater than zero")
for i in range (0,num):
    n1 = n2
    n2 = sum
    sum = n1 + n2
    print( sum, end=" ")


