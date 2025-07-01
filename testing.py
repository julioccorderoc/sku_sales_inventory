import requests
import json

url = "https://damonsun.webhook.office.com/webhookb2/ffbace3f-3691-4bcc-ae50-0810ab083f78@3415c056-27a0-49aa-93c9-0b59dfa997e3/IncomingWebhook/da544aa0febe4f608182c3b2fe2dd3dd/8242f0c3-0820-401c-b7b2-8322c708479c/V24K_CI7TBQUS1FH8jLhuj4ZqO5gehGCFGmI6a3rlyHGg1"

# Example payload, adjust as needed
payload = {"text": "Hello from Python!", "key": "value", "another_key": "another_value"}

headers = {"Content-Type": "application/json"}

try:
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    print(f"Status Code: {response.status_code}")
    print("Response Body:")
    print(response.json())  # Assuming the response is JSON
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
    print(f"Response content: {response.text}")
except requests.exceptions.RequestException as err:
    print(f"An error occurred: {err}")
