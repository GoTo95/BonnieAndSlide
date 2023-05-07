import requests
from io import BytesIO

# URL файла на GitHub
file_url = 'https://rawgithub.com/GoTo95/BonnieAndSlide/blob/master/main.py'

# загрузка содержимого файла
response = requests.get(file_url)
content = BytesIO(response.content)
print(response.content)
# print(content.read())
# # выполнение кода файла
# exec(content.read())
