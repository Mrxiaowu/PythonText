import requests

def main():
    param = {'MobilePhoneNo': '17707082033'}
    resp = requests.get('https://mobile.4001961200.com/pcwap/SmsPasswordApplyForCreditCard.do',param)
    print(resp)

if __name__ == '__main__':
    main()
