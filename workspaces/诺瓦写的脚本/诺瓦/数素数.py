# i = 2
# while (True):
#     j = 2
#     while (j <= (i / j)):
#         if not (i % j): # 判断能不能整除，能整除就直接跳过
#             break
#         j = j + 1
#     if (j > i / j):
#         print(i, " 是素数")
#     i = i + 1
#
# print("Good bye!")

## 一句话完成
print(" ".join("%s" % x for x in range(2,100) if not [y for y in range(2,x) if x % y ==0]))