import requests
import os


# def Test2(rootDir): 按照目录树结构以及按照首字母排序进行输出的。
#     for lists in os.listdir(rootDir):
#         path = os.path.join(rootDir, lists)
#         print(path)
#         if os.path.isdir(path):
#             Test2(path)

if __name__ == '__main__':

        path = "F:\\model\\"  # 文件夹目录
        list_dirs = os.walk(path)
        for root, dirs, files in list_dirs:
            for d in dirs:
                print(os.path.join(root, d))
            for f in files:
                print(os.path.join(root, f))


                            # # 1.0
        # # imageUrl = "http://model.nova3d.cn/up/?only=image"
        # stlUrl = "http://model.nova3d.cn/up/"
        #
        # # imageFile = {'image': open('F:\\model\\593170\\images\\vegetaBust5_preview_featured.jpg', 'rb')}
        # stlFile = {'image': open('F:\\model\\593170\\files\\vegetaBust.stl', 'rb')}
        #
        # # 折中方案，参数按如下方式组织，也是模拟multipart/form-data的核心
        # params = {"upfile": (None)}
        #
        # res = requests.post(stlUrl, files=stlFile,data=params)
        # # 查看请求体是否符合要求，有具体接口可以直接用具体接口，成功则符合要求，
        # # print(res.request.body)
        # # 查看请求头
        # print(res.request.headers)
