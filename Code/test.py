arr = [3,2,5,7,9,1,6]
import random
pivot = random.choice(arr)

left = []
middle = []
right = []
for i in range(len(arr)):
    if arr[i] < pivot:
        left.append(arr[i])
    elif arr[i] == pivot:
        middle.append(arr[i])
    else:
        right.append(arr[i])
arr = left + [pivot] + right
print(arr)