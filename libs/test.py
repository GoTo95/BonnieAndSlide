import requests
from io import BytesIO

# URL файла на GitHub
file_url = 'https://raw.githubusercontent.com/user/repo/branch/path/to/file.py'

# загрузка содержимого файла
response = requests.get(file_url)
content = BytesIO(response.content)

# выполнение кода файла
exec(content.read())
