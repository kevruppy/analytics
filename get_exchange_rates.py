## check for status code/ retries etc.

import requests

DATES = [
    "2022-01-31",
    "2022-02-28",
    "2022-03-31",
    "2022-04-30",
    "2022-05-31",
    "2022-06-30",
    "2022-07-31",
    "2022-08-31",
    "2022-09-30",
    "2022-10-31",
    "2022-11-30",
    "2022-12-31",
    "2023-01-31",
    "2023-02-28",
    "2023-03-31",
    "2023-04-30",
    "2023-05-31",
    "2023-06-30",
    "2023-07-31",
    "2023-08-31",
    "2023-09-30",
    "2023-10-31",
    "2023-11-30",
    "2023-12-31",
]

res = []
for date in DATES:
    url = f"https://api.frankfurter.app/{date}"
    response = requests.get(url=url, params={"amount": 1, "from": "EUR", "to": "USD"})
    res.append(response.json())

print(res)
