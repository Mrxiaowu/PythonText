# -*- coding:UTF-8 -*-
from bs4 import BeautifulSoup
import requests,zipfile, io
# from concurrent.futures import ThreadPoolExecutor

if __name__ == '__main__':
        server = "https://www.thingiverse.com"
        target = 'https://www.thingiverse.com/KryptonicLoser/collections/bow-9-9-9-15'  #2.0
        # target = 'https://www.thingiverse.com/search?q=bleach&dwh=555bd6c1f0a8fca' # 3.0

        req = requests.get(url=target)
        html = req.text
        div_bf = BeautifulSoup(html)

        # 1.0 直接爬取所有的div的href
        # div = div_bf.find_all('div', class_="items-page result-page justify justify-left")
        # print(div[0])
        # print("--------------------------------")
        #
        # a_bf = BeautifulSoup(str(div[0]))
        # a = a_bf.find_all('a')
        #
        # for each in a:
        #         if each.get('href') == "/FLOWALISTIK":
        #                 print('success')
        #                 continue
        #         print(server + each.get('href'))

        # 2.0 获取https://www.thingiverse.com/KryptonicLoser/collections/bow-9-9-9-15 这系列页面的数据
        # div = div_bf.find_all('a', class_="card-img-holder")
        # a = 0
        # for each in div:
        #         a = a + 1
        #         print(server + each.get('href') + "/zip")
        #         response = requests.get(server + each.get('href') + "/zip")
        #
        #         print(response.headers)
        #         z = zipfile.ZipFile(io.BytesIO(response.content))
        #         z.extractall("F:\\model\\"+str(a))
        #         print("----------------------------")

        # 3.0 准确获取《a》里的href
        div = div_bf.find_all('a', class_="card-img-holder")
        a = 0
        # pool = ThreadPoolExecutor(10)
        for each in div:
                # a = a + 1
                href = each.get("href")
                fileName = href.split(':')[1]
                print(server + href + "/zip")
                response = requests.get(server + each.get('href') + "/zip")
                print(response.headers)
                z = zipfile.ZipFile(io.BytesIO(response.content))
                z.extractall("F:\\model\\" + str(fileName))
                print("----------------------------")





