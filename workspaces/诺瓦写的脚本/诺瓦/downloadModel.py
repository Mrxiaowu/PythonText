# -*- coding:UTF-8 -*-
from bs4 import BeautifulSoup
import requests,zipfile, io , time
from concurrent.futures import ThreadPoolExecutor

# 因为每页只显示12个数据，即第一页的数据，想要获取第二页的数据必须得发送一个post的请求，post中
# 的body中带的参数可能需要修改一下内容。
totalFile = 199  # 总文件数
last_page = 17   #总页数，以及最后一页的页码
per_page = 12   #每页存放的文件数
indexPage = 1  #从第几页开始爬

def download(downloadUrl,a):
        response = requests.get(downloadUrl)
        # print(response.headers)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall("F:\\model\\" + str(a))


# 3.0 准确获取《a》里的href
def getDownloadUrl(label,indexPage):
        print("当前正在爬取第" + str(indexPage) + "页的数据")
        a = 0
        pool = ThreadPoolExecutor(10)
        for each in label:
                # a = a + 1
                href = each.get("href")
                fileName = href.split(':')[1]
                print("============="+server + href + "/zip")
                downloadUrl = server + each.get('href') + "/zip"
                pool.submit(download, downloadUrl, fileName)  # Submit work to the pool
        time.sleep(2) # 每次爬的时候加一个2s延时，因为下载速度不可能那么快，我们尽量友好一些。

        if indexPage <= last_page:
                indexPage += 1
                getDownloadUrl(sendNextPageUrl(indexPage).find_all('a', class_="card-img-holder"),indexPage)
        print("爬取完毕")
        return


# 4.0 发送post请求
def sendNextPageUrl(indexPage):
        postData = {"extra_path": "?type=things&q=dragon+ball&sort=relevant&hide_customized=1&dwh=405bd826182c9ce",
                    "per_page": str(per_page),
                    "total": str(totalFile),
                    "type": "things",
                    "q": "dragon ball",
                    "sort": "relevant",
                    "hide_customized": "true",
                    "dwh": "405bd826182c9ce",
                    "base_url": "/search/",
                    "auto_scroll": "true",
                    "page": str(indexPage),
                    "last_page": str(last_page),
                    "$container": ".results-container",
                    "source": "/ajax/search/results/"}
        postReq = requests.post("https://www.thingiverse.com/ajax/search/results/", data=postData)
        html = postReq.text
        div_bf = BeautifulSoup(html)
        return div_bf


if __name__ == '__main__':
        server = "https://www.thingiverse.com"
        target = 'https://www.thingiverse.com/search?q=dragon+ball&dwh=405bd826182c9ce'  #2.0

        req = requests.get(url=target)
        html = req.text
        div_bf = BeautifulSoup(html)

        div = div_bf.find_all('a', class_="card-img-holder")
        getDownloadUrl(div,indexPage)

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





