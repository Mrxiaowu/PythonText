# -*- coding: utf-8 -*-
"""
@author: barry
"""

import os
import time
from urllib.request import urlopen
from urllib import request
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver

def find_cat_url(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
    req = request.Request(url,headers=headers)
    html = urlopen(req)
    bsObj = BeautifulSoup(html.read(),"html.parser")
    bs = bsObj.find('div',attrs={'class':'hot-list clearfix'}).find_all('dt')
    cat_url =[]
    cat_name = []

    for i in range(len(bs)):
        for j in range(len(bs[i].find_all('a'))):
            print(bs[i].find_all('a')[j])
            # print(bs[i].find_all('a')[j].text)
            cat_url.append(bs[i].find_all('a')[j].attrs['href'])
            cat_name.append(bs[i].find_all('a')[j].text)
    cat_url = ['http://www.mafengwo.cn'+cat_url[i] for i in range(0,len(cat_url))]

    # for i in range(len(cat_url)):
    #     print(cat_url[i])

    return cat_url



## 获得城市url地址
def find_city_url(url_list):
    city_name_list = []
    city_url_list = []
    for i in range(len(url_list)):
        driver = webdriver.Chrome(executable_path="C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe")
        driver.maximize_window()
        # 不加s无法打开网页--多亏我敏锐的嗅觉！
        url = url_list[i].replace('travel-scenic-spot/mafengwo','mdd/citylist').replace('http', 'https')
        print(" 每个url "+url)
        driver.get(url)
        while True:
            try:
                time.sleep(1)
                bs = BeautifulSoup(driver.page_source,'html.parser')
                url_set = bs.find_all('a',attrs={'data-type':'目的地'})
                city_name_list = city_name_list + [url_set[i].text.replace('\n','').split()[0] for i in range(len(url_set))]
                city_url_list = city_url_list + [url_set[i].attrs['data-id'] for i in range(len(url_set))]
                print(" 城市 " + str(city_name_list))
                print(" 城市编号 " + str(city_url_list))
                js = "var q=document.documentElement.scrollTop=800"
                driver.execute_script(js)
                time.sleep(2)
                driver.find_element_by_class_name('pg-next').click()

            except Exception as e:
                print(str(e))
                break
            driver.close()
        return city_name_list,city_url_list


url = 'http://www.mafengwo.cn/mdd/'
url_list = find_cat_url(url)
city_name_list,city_url_list=find_city_url(url_list)
city = pd.DataFrame({'city':city_name_list,'id':city_url_list})




# 这样会提示权限问题，用下面那个直接路径
# browser = webdriver.Chrome("C:\Program Files (x86)\Google\Chrome\Application")
# browser = webdriver.Chrome(executable_path="C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe")
# browser.get('http://www.baidu.com/')
# js = "var q=document.documentElement.scrollTop=800"
# browser.command_executor(js)