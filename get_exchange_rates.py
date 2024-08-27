import requests

url = "https://api.frankfurter.app/2023-12-31?amount=10&from=GBP&to=USD"

# end of month reporting (24 requests -> no throttling, eur->usd)

response = requests.get(url=url, params={"amount": 1, "from": "EUR", "to": "USD"})

print(response.text)
