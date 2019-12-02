# coding:utf-8
import itertools as its

# 迭代器
words = "1234567890"
# 生成密码本的位数，五位数，repeat=5
r = its.product(words, repeat=8)
# 保存在文件中，追加
dic = open("C:/Users/shenpeijia/Desktop/password.txt", "a")
# i是元组
for i in r:
    # jion空格链接
    dic.write("".join(i))
    dic.write("".join("\n"))
    print(i)
dic.close()
print("密码本已生成")
