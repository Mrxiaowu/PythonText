"""

据说itchat容易被封号，还是封存了吧

"""

# 给文件助手发信息
import itchat
#
itchat.auto_login()
# itchat.send('Hello, filehelper11', toUserName='filehelper')


friendList = itchat.get_friends(update=True)[1:]
for friend in friendList:
    # 如果是演示目的，把下面的方法改为print即可
    # itchat.send(SINCERE_WISH % (friend['DisplayName']
    #     or friend['NickName']), friend['UserName'])
    # 需要遍历朋友找到nickname  然后找到username
    print(friend['UserName'] + " " +friend['NickName'])
    if (friend['NickName']  == 'Sugar佳15979086491') :
        print("???" + friend['UserName'])
        itchat.send('测试 Hello, Sugar佳15979086491', toUserName=friend['UserName'])


# 用微信扫一扫后登陆就可以别人给你发消息，你这边也返回对应返回得消息
# @itchat.msg_register(itchat.content.TEXT)
# def text_reply(msg):
#     print(msg)
#     return 'Text' + msg['MsgId']
#     return msg
# itchat.auto_login()   # 跳出二维码微信登陆
# itchat.run()  # 可能是一只运行的命令


# @itchat.msg_register(itchat.content.TEXT)
# def text_reply(msg):
#     print(msg)
#     print("用户信息 "+msg.fromUserName)
#     msg.user.send('%s: %s' % (msg.type, msg.text))
#
# # itchat.auto_login(enableCmdQR=2) # 二维码登陆，太大了 忽略
# itchat.auto_login(hotReload=True)
# itchat.run()

