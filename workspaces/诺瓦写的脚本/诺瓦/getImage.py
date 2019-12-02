# -*- coding:UTF-8 -*-
import requests

if __name__ == '__main__':
    target = '/napi/collections/1065976/'
    headers = {'authorization': 'your Client-ID'}
    req = requests.get(url=target, headers=headers, verify=False)
    print(req.text)