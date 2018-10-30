# -*- coding:UTF-8 -*-
from bs4 import BeautifulSoup
import requests,zipfile, io
from concurrent.futures import ThreadPoolExecutor


# def download(downloadUrl,a):
#         response = requests.get(downloadUrl)
#         print(response.headers)
#         z = zipfile.ZipFile(io.BytesIO(response.content))
#         z.extractall("F:\\model\\" + str(a))
#         print("----------------------------")

if __name__ == '__main__':
        # server = "https://www.thingiverse.com"
        # target = 'https://www.thingiverse.com/search?q=dragon+ball&dwh=405bd826182c9ce'  #2.0
        #
        # req = requests.get(url=target)
        # html = req.text
        # div_bf = BeautifulSoup(html)

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
        # div = div_bf.find_all('a', class_="card-img-holder")
        # a = 0
        # pool = ThreadPoolExecutor(12)
        # for each in div:
        #         a = a + 1
        #         href = each.get("href")
        #         fileName = href.split(':')[1]
        #         print(server + href + "/zip")
        #         downloadUrl = server + each.get('href') + "/zip"
        #         pool.submit(download, downloadUrl,a)  # Submit work to the pool

        # 4.0 发送post请求
        postData = {"extra_path": "?type=things&q=dragon+ball&sort=relevant&hide_customized=1&dwh=405bd826182c9ce",
"per_page": "12",
"total": "199",
"type": "things",
"q": "dragon ball",
"sort": "relevant",
hide_customized: true,
dwh: 405bd826182c9ce,
base_url: /search/,
auto_scroll: true,
page: 4,
last_page: 17,
$container: .results-container,
source: /ajax/search/results/}
        postReq = requests.post("https://www.thingiverse.com/ajax/search/results/",data=postData)
        # html = postReq.text

