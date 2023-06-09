import requests

try:
  response = requests.get('https://api.ipify.org/?format=json')
  print("Public IP address :", response.json()['ip'])
except requests.RequestException:
  print("Unable to retrieve public IP address")
