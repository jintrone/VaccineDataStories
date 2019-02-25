import requests

url="https://www.naturalnews.com/index_1_1_43.html"
test = requests.get(url)
test.encoding = 'utf-8'
with open("test.html","w") as f:
    f.write(test.text)
