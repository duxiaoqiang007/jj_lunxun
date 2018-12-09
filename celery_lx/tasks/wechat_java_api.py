import requests

def get_wechat_java():
    r = requests.get('http://localhost:8080/getPython');
    r.ecoding='utf-8'
    content = r.text;
    print(content);