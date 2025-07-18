import requests
import json

r = requests.get("https://api.github.com//repos/asadjamal98/Projects-Portfolio/commits")
r.raise_for_status
#print(json.dumps(r.json(), indent=2))
result = r.json()
print(len(result))
for x in result:
    print(x.get("sha"))

