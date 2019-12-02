import os
import datetime
import requests
import xlwt
import time
from pyecharts.charts import Bar
from pyecharts import options as opts

class maoyan():

    def __init__(self):
        self.headers = {
            'Host': 'piaofang.maoyan.com',
            'Referer': 'https://piaofang.maoyan.com/dashboard',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36 LBBROWSER',
            'X-Requested-With': 'XMLHttpRequest'
        }

    def get_page(self):
        url = 'https://box.maoyan.com/promovie/api/box/second.json'
        try:
            response = requests.get(url, self.headers)
            if response.status_code == 200:
                return response.json()
        except requests.ConnectionError as e:
            print('Error', e.args)

    def parse_page(self, json):
        if json:
            data = json.get('data')
            # print("data: "+str(data.get('list')[0]))
            dimensions = ['avgSeatView', 'avgShowView', 'avgViewBox', 'boxInfo', 'boxRate', 'movieName', 'releaseInfo',
                          'seatRate', 'showInfo', 'showRate', 'sumBoxInfo']

            self.piaofang = {}
            item = data.get('list')[0]
            for dimension in dimensions:
                self.piaofang[dimension] = item.get(dimension)
            return self.piaofang

            # 原代码
            # for index, item in enumerate(data.get('list')):
            #     self.piaofang = {}
            #     for dimension in dimensions:
            #         self.piaofang[dimension] = item.get(dimension)
            #     yield self.piaofang

    def main(self, moviename):

        my_time = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '15:41', '%Y-%m-%d%H:%M')

        avgSeatView, avgShowView, avgViewBox, boxInfo, \
        boxRate, movieName, releaseInfo, seatRate, \
        showInfo, showRate, sumBoxInfo = [], [], [], [], [], [], [], [], [], [], []

        boxInfoBarX = []
        file_name = 'F:/test.xls'
        wbk = xlwt.Workbook()  # 初始化workbook对象
        sheet1 = wbk.add_sheet(moviename + "票房情况", cell_overwrite_ok=True)

        while True:
            now_time = datetime.datetime.now()
            print("现在时间：" + str(now_time))

            now = time.strftime("%H:%M:%S")
            print(now)
            boxInfoBarX.append(time.strftime("%H:%M:%S"))

            json = self.get_page()
            results = self.parse_page(json)
            avgSeatView.append(results.get('avgSeatView'))
            avgShowView.append(results.get('avgShowView'))
            avgViewBox.append(results.get('avgViewBox'))
            boxInfo.append(results.get('boxInfo'))
            boxRate.append(results.get('boxRate'))
            releaseInfo.append(results.get('releaseInfo'))
            seatRate.append(results.get('seatRate'))
            showInfo.append(results.get('showInfo'))
            showRate.append(results.get('showRate'))
            sumBoxInfo.append(results.get('sumBoxInfo'))

            print("票房boxInfo为: " + str(boxInfo))
            if now_time > my_time:
                print("超过指定时间，停止" + str(now_time))
                break
            time.sleep(4)

        print("票房liebiao "+str(boxInfo))
        sheet1.write(0, 0, "实时票房(万)") #boxInfo  write第一个参数是行，第二个参数为列
        sheet1.write(0, 1, "总票房")#sumBoxInfo
        sheet1.write(0, 2, "场均上座率")#avgSeatView
        sheet1.write(0, 3, "场均人次")#avgShowView
        sheet1.write(0, 4, "平均票价")#avgViewBox
        sheet1.write(0, 5, "票房占比")#boxRate
        sheet1.write(0, 6, "上映天数")#releaseInfo
        sheet1.write(0, 7, "座位费率")#seatRate
        sheet1.write(0, 8, "排片场次")#showInfo
        sheet1.write(0, 9, "排片占比")#showRate

        for i in range(0, len(avgSeatView)):
            sheet1.write(i + 1, 0, boxInfo[i])
            sheet1.write(i + 1, 1, sumBoxInfo[i])
            sheet1.write(i + 1, 2, avgSeatView[i])
            sheet1.write(i + 1, 3, avgShowView[i])
            sheet1.write(i + 1, 4, avgViewBox[i])
            sheet1.write(i + 1, 5, boxRate[i])
            sheet1.write(i + 1, 6, releaseInfo[i])
            sheet1.write(i + 1, 7, seatRate[i])
            sheet1.write(i + 1, 8, showInfo[i])
            sheet1.write(i + 1, 9, showRate[i])
        wbk.save(file_name)
        {
            Bar()
                .set_global_opts(title_opts=opts.TitleOpts(title="我的第一个图表", subtitle="我是副标题"))
                .
        }

if __name__ == "__main__":
    moviename = "哪吒之魔童降世"
    my = maoyan()
    my.main(moviename)

