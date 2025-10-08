import requests

#Use this Python script with care. It can remove complete streams from the database.
#It is used to remove faulty data or depracated streams.

#Input variables:
variable_id = '<variable_id>'                               #String of variable ID
API_KEY = '<ApiKey>'    #ApiKey

#No need to change beyond this point
HOST_KL = 'https://maq-observations.nl'
END_POINT = '/wp-json/maq/v1/streams/'
headers = {
    'Accept': 'application/json',
    'Authorization': f'ApiKey {API_KEY}'}

response = requests.delete(HOST_KL+END_POINT+variable_id, headers=headers)

if response.status_code == 200 or response.status_code == 204:
    print("Variable deleted successfully.")
else:
    print(f"Failed to delete variable. Status code: {response.status_code}")
    print(f"Response: {response.text}")


#Streams to delete (and refill), 2024-10-23 + 2024-10-24 downtime:
#IF DELETING PER TIMESTAMP NOT POSSIBLE!
#48649, 48650, 48653, 48658, 48659, 48660, 48662, 48663, 48678, 48679, 48680, 48681, 48682, 48683, 48684, 48685, 48686, 48687, 49278, 49282
