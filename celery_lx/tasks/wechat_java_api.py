import requests

def get_wechat_java():
    # r = requests.get('http://localhost:8080/getPython');
    # r = requests.get('http://192.168.91.128:8080/jiujiang-main-entrance-server-0.1.0/getPython')
    # r = requests.get('http://sgjj.jj-port.com:2004/jiujiang-wechat-0.1.0/getPython', headers={'Connection': 'close'})
    # r.ecoding='utf-8'
    # content = r.text;
    # s = requests.session()
    # s.keep_alive = False
    # print(content);

    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False
    s.ecoding = 'utf-8'
    # s.get('http://sgjj.jj-port.com:2004/jiujiang-wechat-0.1.0/getPython', headers={'Connection': 'close'})
    r = s.get('http://127.0.0.1:2004/jiujiang-wechat-0.1.0/getPython', headers={'Connection': 'close'})
    print(r.text)